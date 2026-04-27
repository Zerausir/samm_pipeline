"""
main_raw_voice.py
─────────────────
ETL de datos de voz crudos → voice_raw_measurements (regulatorio / Grafana).

Características:
  - Sin filtro de SessionType en table1: se conservan todos (MO, MT, etc.).
    La vista grafana_voice_geo_view filtra CallDirection = 'MO'.
  - Sin dropna de coordenadas.
  - Sin dropna final.

Fix 2026-03-23 — cambio arquitectural:
  ANTES: table4 (SessionVoiceQuality) era la base del merge.
         table4 solo contiene llamadas que generaron scores AQM, es decir,
         llamadas establecidas. Las llamadas fallidas, bloqueadas y caídas
         nunca generan registros en table4 → se perdían del pipeline raw.

  AHORA: table3 (SessionSummaryVoice) es la base del merge.
         table3 contiene TODAS las llamadas (establecidas, fallidas,
         bloqueadas, caídas) — exactamente como el Dashboard de Grafana
         en SQL Server que parte de cdr.SessionSummaryVoice.
         table4 ya no se usa en el pipeline raw porque las métricas AQM
         necesarias (AqmSessionEndAqmCallQuality, ...Downlink, ...Uplink)
         provienen de table3, no de table4.

Nuevo orden de merges:
  table3 (SessionSummaryVoice) ← BASE (todas las llamadas)
      LEFT JOIN table1 (SessionSummary)  on [DatasourceId, CallIndex]
      LEFT JOIN sense_nacional           on [IMSI, IMEI]
"""

import gc
import os
from datetime import timedelta, datetime

import pandas as pd
from app.utils.postgres_handler import get_postgres_handler


# ---------------------------------------------------------------------------
# Helpers de entorno
# ---------------------------------------------------------------------------

def get_env_var(var_name, default=None):
    value = os.environ.get(var_name)
    if value is None:
        from dotenv import load_dotenv
        load_dotenv()
        value = os.environ.get(var_name)
    return value if value is not None else default


# ---------------------------------------------------------------------------
# Carga de datos
# ---------------------------------------------------------------------------

def load_voice_data():
    data_dir = '/opt/airflow/app/data'
    voice_files = {
        'table1': f"{data_dir}/extract_voz_table1.parquet",
        'table3': f"{data_dir}/extract_voz_table3.parquet",
        'sense_nacional': f"{data_dir}/sense_nacional_v0.xlsx",
    }

    missing = [p for p in voice_files.values() if not os.path.exists(p)]
    if missing:
        print(f"ERROR: Archivos no encontrados: {', '.join(missing)}")
        return None

    dataframes = {}
    for key in ['table1', 'table3']:
        try:
            print(f"Leyendo: {voice_files[key]}")
            dataframes[key] = pd.read_parquet(voice_files[key])
            print(f"  {key}: {len(dataframes[key]):,} registros")
        except Exception as e:
            print(f"Error cargando {voice_files[key]}: {e}")
            return None

    try:
        print(f"Leyendo: {voice_files['sense_nacional']}")
        dataframes['sense_nacional'] = pd.read_excel(voice_files['sense_nacional'])
        print(f"  sense_nacional: {len(dataframes['sense_nacional']):,} registros")
    except Exception as e:
        print(f"Error cargando sense_nacional: {e}")
        return None

    return dataframes


# ---------------------------------------------------------------------------
# table1 — SessionSummary
# Sin filtro de SessionType
# ---------------------------------------------------------------------------

def _process_table1_raw(df):
    """
    Normaliza SessionSummary sin filtrar por SessionType.
    Aporta: StartTime, EndTime, SimOperator, Operator, IMSI, IMEI,
            StartLatitude/Longitude, EndLatitude/Longitude,
            StartRadioTechnology, EndRadioTechnology al merge con table3.
    """
    print("Procesando SessionSummary (raw — todos los SessionType)...")

    required_columns = [
        'DatasourceId', 'SessionIdOrCallIndex', 'SessionType', 'StartTime',
        'StartLatitude', 'StartLongitude', 'StartRadioTechnology',
        'EndSessionNetworkIndex', 'EndTime', 'EndLatitude', 'EndLongitude',
        'EndRadioTechnology', 'Operator', 'SimOperator', 'IMSI', 'IMEI',
        'SessionEndStatus', 'ErrorCause', 'ErrorCauseDetails',
        'RadioTechnologySequence', 'NoGps',
    ]
    existing = [c for c in required_columns if c in df.columns]
    df = df[existing].copy()

    missing_cols = [c for c in required_columns if c not in existing]
    if missing_cols:
        print(f"  ⚠️  Columnas no encontradas en table1: {missing_cols}")

    dup_cols = [c for c in [
        'DatasourceId', 'SessionType', 'StartRadioTechnology', 'IMSI', 'IMEI',
        'StartLatitude', 'StartLongitude', 'EndLatitude', 'EndLongitude',
        'StartTime', 'EndTime', 'EndRadioTechnology', 'Operator', 'SimOperator',
    ] if c in df.columns]
    if dup_cols:
        before = len(df)
        df = df.drop_duplicates(subset=dup_cols, keep='first')
        print(f"  Dedup table1: {before:,} → {len(df):,}")

    for col in ['StartTime', 'EndTime']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce',
                                     format='%Y-%m-%d %H:%M:%S.%f')
    for col in ['StartLatitude', 'StartLongitude', 'EndLatitude', 'EndLongitude']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
    for col in ['DatasourceId', 'IMSI', 'IMEI', 'EndSessionNetworkIndex']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    if 'NoGps' in df.columns:
        df['NoGps'] = df['NoGps'].astype('boolean')

    if 'SessionType' in df.columns:
        print(f"  SessionType distribución: {df['SessionType'].value_counts().to_dict()}")

    # Renombrar para el merge con table3
    if 'SessionIdOrCallIndex' in df.columns:
        df = df.rename(columns={'SessionIdOrCallIndex': 'CallIndex'})

    print(f"  table1 raw lista: {len(df):,} filas")
    return df


# ---------------------------------------------------------------------------
# table3 — SessionSummaryVoice (BASE del pipeline raw)
# Contiene TODAS las llamadas: establecidas, fallidas, bloqueadas, caídas
# FIX 2026-03-20: agregados CallDroppedDateTime y CallBlockedDateTime
# ---------------------------------------------------------------------------

def _process_table3(df):
    """
    Normaliza SessionSummaryVoice.
    Es la BASE del merge raw — contiene todas las llamadas.
    Aporta todas las métricas de voz para los dashboards regulatorios.
    """
    print("Procesando SessionSummaryVoice (base del merge raw)...")

    required_columns = [
        'DatasourceId', 'CallIndex', 'CallDirection', 'AqmCallType',
        'StartRadioTechnology', 'EndRadioTechnology', 'DialStartDateTime',
        'DialStartPhoneNumber', 'CallAttemptDateTime', 'CallAttemptRadioTechnology',
        'CallAttemptCsfbDateTime', 'CallAttemptCsfRadioTechnology',
        'CallEstablishedDateTime', 'CallEstablishedRadioTechnology',
        'CallEstablishedDomain', 'DialEndDateTime', 'DialEndServiceStatus',
        'CallInitiationDateTime', 'CallInitiationRadioTechnology',
        'CallReestablishedDateTime', 'CallEndDateTime', 'CallEndRadioTechnology',
        'CallEndCause', 'CallEndType', 'CallEndDomain', 'CallEndCallDuration',
        'AqmSessionEndOtherPartyPhoneNumber', 'AqmSessionEndPhoneNumber',
        'AqmSessionEndAqmCallQuality',  # métrica para mapa
        'AqmSessionEndAqmCallQualityDownlink',  # métrica para mapa
        'AqmSessionEndAqmCallQualityUplink',  # métrica para mapa
        'AqmAlgorithmDownlink', 'AqmAlgorithmUplink',
        'SpeechCodecs', 'SpeechPathDelayOneWay', 'SilentCall',
        'SpeechInterruptionTimeDownlinkDuration',
        'RtpInterruptionTimeAudioInterruptionTime',
        'HandoverSpeechInterruptionDownlinkDuration', 'CallSetupOffHookTime',
        'CallAttemptAccessNetworkInfo', 'CallSetupAccessNetworkInfo',
        'CallSetupDomain', 'CallSetupUserPerceivedCallSetupTime',
        'CallEstablishedUserCallEstablishedTime', 'CallEstablishedCallAnswerDelay',
        'BearerTechnology',
        'CallDroppedDateTime',  # FIX 2026-03-20
        'CallBlockedDateTime',  # FIX 2026-03-20
    ]
    existing = [c for c in required_columns if c in df.columns]
    df = df[existing].copy()

    missing_cols = [c for c in required_columns if c not in existing]
    if missing_cols:
        print(f"  ⚠️  Columnas no encontradas en table3: {missing_cols}")

    dup_cols = [c for c in [
        'DatasourceId', 'StartRadioTechnology', 'EndRadioTechnology',
        'DialStartDateTime', 'CallAttemptDateTime', 'CallAttemptRadioTechnology',
        'CallEndDateTime', 'CallEndRadioTechnology', 'CallEndCause',
    ] if c in df.columns]
    if dup_cols:
        before = len(df)
        df = df.drop_duplicates(subset=dup_cols, keep='first')
        print(f"  Dedup table3: {before:,} → {len(df):,}")

    datetime_cols = [
        'DialStartDateTime', 'CallAttemptCsfbDateTime', 'CallSetupDateTime',
        'CallSetupCsfbDateTime', 'CallEstablishedDateTime', 'DialEndDateTime',
        'CallInitiationDateTime', 'CallEndDateTime',
        'EutranReselectionTimeAfterCsfbCallDateTime',
        'CallAttemptDateTime', 'CallBlockedDateTime', 'CallReestablishedDateTime',
        'CallDroppedDateTime',
    ]
    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce',
                                     format='%Y-%m-%d %H:%M:%S.%f')

    def convertir_a_duracion(valor):
        try:
            valor = str(valor).strip()[:15]
            t = datetime.strptime(valor, "%H:%M:%S.%f")
            return timedelta(hours=t.hour, minutes=t.minute,
                             seconds=t.second, microseconds=t.microsecond)
        except Exception:
            return None

    duration_cols = [
        'CallSetupTime', 'CallSetupUserPerceivedTime', 'CallSetupServiceRequestTime',
        'CallSetupCsfbTime', 'CallSetupCsfbUserPerceivedTime',
        'CallSetupCsfbServiceRequestTime', 'CallEndCallDuration',
        'EutranReselectionTimeAfterCsfbCallIdleToLteTime',
        'CallSetupOffHookTime', 'CallSetupUserPerceivedCallSetupTime',
        'CallEstablishedUserCallEstablishedTime', 'CallBlockedDuration',
        'CallBlockedCsfbDuration',
    ]
    for col in duration_cols:
        if col in df.columns:
            df[col] = df[col].apply(convertir_a_duracion)

    for col in ['AqmSessionEndAqmCallQuality', 'AqmSessionEndAqmCallQualityDownlink',
                'AqmSessionEndAqmCallQualityUplink']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)

    for col in ['DatasourceId', 'DialStartPhoneNumber', 'DialEndSampleId',
                'AqmSessionEndOtherPartyPhoneNumber', 'AqmSessionEndPhoneNumber']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    if 'CallDirection' in df.columns:
        print(f"  CallDirection distribución: {df['CallDirection'].value_counts().to_dict()}")
    if 'DialEndServiceStatus' in df.columns:
        print(f"  DialEndServiceStatus distribución: {df['DialEndServiceStatus'].value_counts().to_dict()}")

    print(f"  table3 lista: {len(df):,} filas")
    return df


# ---------------------------------------------------------------------------
# sense_nacional
# ---------------------------------------------------------------------------

def _process_sense_nacional(df):
    print("Procesando sense_nacional...")
    df['IMSI'] = pd.to_numeric(df['IMSI'], errors='coerce').astype('Int64')
    df['IMEI'] = pd.to_numeric(df['IMEI'], errors='coerce').astype('Int64')
    return df


# ---------------------------------------------------------------------------
# Merge raw — table3 como BASE
# ---------------------------------------------------------------------------

def process_raw_voice_data(dataframes, chunk_size=8_000):
    """
    Merge con table3 (SessionSummaryVoice) como base.

    Orden:
      MERGE 1: table3 + table1  on [DatasourceId, CallIndex]  (chunks)
      MERGE 2: resultado + sense_nacional  on [IMSI, IMEI]

    SIN dropna — todas las llamadas son datos regulatorios válidos.
    """
    print("🚀 INICIANDO PROCESO RAW VOICE (table3 como base)")
    print("=" * 70)

    df1 = dataframes['table1'].copy()
    df3 = dataframes['table3'].copy()
    df4 = dataframes['sense_nacional'].copy()
    del dataframes
    gc.collect()

    print(f"Table1 (SessionSummary)      : {df1.shape}")
    print(f"Table3 (SessionSummaryVoice) : {df3.shape}  ← base")
    print(f"sense_nacional               : {df4.shape}")

    try:
        # PASO 1: normalización
        print("\n📞 PASO 1: Normalizando tipos...")
        df1 = _process_table1_raw(df1)
        df3 = _process_table3(df3)
        df4 = _process_sense_nacional(df4)

        if df3.empty:
            raise ValueError("❌ Table3 (SessionSummaryVoice) vacía tras normalización")
        if df1.empty:
            raise ValueError("❌ Table1 (SessionSummary) vacía tras normalización")

        # PASO 2: merge en chunks table3 (base) + table1
        print("\n📊 PASO 2: Merge table3 (base) + table1 en chunks...")
        print(f"  Merge key: [DatasourceId, CallIndex]")

        merge_cols = ['DatasourceId', 'CallIndex']
        processed_chunks = []
        num_chunks = len(df3) // chunk_size + (1 if len(df3) % chunk_size else 0)
        failed_chunks = 0

        print(f"  {len(df3):,} registros en {num_chunks} chunks de {chunk_size:,}")

        for i in range(0, len(df3), chunk_size):
            chunk_num = i // chunk_size + 1
            chunk_df3 = df3.iloc[i: i + chunk_size].copy()
            try:
                on_cols = [c for c in merge_cols
                           if c in chunk_df3.columns and c in df1.columns]
                chunk_merged = chunk_df3.merge(df1, on=on_cols, how='left',
                                               suffixes=('', '_df1'))
                if len(chunk_merged) > 0:
                    processed_chunks.append(chunk_merged)
                print(f"  Chunk {chunk_num}/{num_chunks}: "
                      f"{len(chunk_df3):,} → {len(chunk_merged):,} filas")
            except Exception as e:
                failed_chunks += 1
                print(f"  ⚠️ Error chunk {chunk_num}: {e}")
                if failed_chunks > num_chunks * 0.1:
                    raise ValueError(f"❌ Demasiados chunks fallidos ({failed_chunks})")
            finally:
                del chunk_df3
                gc.collect()

        if not processed_chunks:
            raise ValueError("❌ Ningún chunk procesado exitosamente")

        df3_1_merged = pd.concat(processed_chunks, ignore_index=True)
        del processed_chunks, df3, df1
        gc.collect()
        print(f"✅ Tras merge 1: {len(df3_1_merged):,} filas")

        # PASO 3: merge resultado + sense_nacional
        print("\n📱 PASO 3: Merge resultado + sense_nacional (IMSI + IMEI)...")
        dataset_final = df3_1_merged.merge(df4, on=['IMSI', 'IMEI'], how='left',
                                           suffixes=('', '_df4'))
        print(f"✅ Tras merge 2 (final): {len(dataset_final):,} filas")

        del df3_1_merged, df4
        gc.collect()

        # SIN dropna
        print("\n✅ Sin filtrado de coordenadas ni estado (raw regulatorio completo)")

        # Resumen de métricas críticas
        print(f"\n🎯 RESUMEN FINAL")
        print("=" * 70)
        print(f"📊 Dataset: {len(dataset_final):,} filas × {len(dataset_final.columns)} columnas")
        print(f"\n📋 VERIFICACIÓN COLUMNAS CRÍTICAS:")

        for col in [
            'CallAttemptDateTime', 'CallDroppedDateTime', 'CallBlockedDateTime',
            'CallEstablishedDateTime', 'DialEndServiceStatus',
            'SimOperator', 'StartTime', 'EndTime',
            'AqmSessionEndAqmCallQuality',
            'AqmSessionEndAqmCallQualityDownlink',
            'AqmSessionEndAqmCallQualityUplink',
        ]:
            if col in dataset_final.columns:
                non_null = dataset_final[col].notna().sum()
                pct = non_null / len(dataset_final) * 100 if len(dataset_final) > 0 else 0
                print(f"   {col}: {non_null:,} no-nulos ({pct:.1f}%)")
            else:
                print(f"   ⚠️  {col}: columna NO presente en dataset final")

        if 'DialEndServiceStatus' in dataset_final.columns:
            print(f"\n   DialEndServiceStatus distribución:")
            dist = dataset_final['DialEndServiceStatus'].value_counts(dropna=False)
            for val, cnt in dist.items():
                print(f"     {val}: {cnt:,}")

        return dataset_final

    except Exception as e:
        print(f"❌ Error in process_raw_voice_data: {e}")
        gc.collect()
        raise


# ---------------------------------------------------------------------------
# Validación
# ---------------------------------------------------------------------------

def validate_raw_voice_data(df):
    if df.empty:
        raise ValueError("Dataset raw de voz vacío")
    if 'DatasourceId' not in df.columns or df['DatasourceId'].isnull().all():
        raise ValueError("DatasourceId ausente o completamente nulo")
    if 'DialEndServiceStatus' not in df.columns:
        raise ValueError("DialEndServiceStatus ausente — métricas de voz no disponibles")
    print(f"✅ Validación raw voz OK: {len(df):,} filas × {len(df.columns)} columnas")
    return True


def _is_empty_data_error(exc: Exception) -> bool:
    """Devuelve True si la excepción indica simplemente que no hay datos en el período."""
    msg = str(exc).lower()
    return any(kw in msg for kw in ("empty", "is empty", "vac", "no data", "0 records"))


# ---------------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------------

def main():
    print("Iniciando ETL de datos de voz CRUDOS (table3 como base)...")

    postgres_handler = get_postgres_handler(
        host=get_env_var('POSTGRES_HOST'),
        port=get_env_var('POSTGRES_PORT'),
        database=get_env_var('POSTGRES_DB'),
        user=get_env_var('POSTGRES_USER'),
        password=get_env_var('POSTGRES_PASSWORD'),
    )
    print(f"Conexión PostgreSQL: {get_env_var('POSTGRES_HOST')}:{get_env_var('POSTGRES_PORT')}")

    postgres_handler.create_raw_tables()
    print("Tablas raw verificadas/creadas")

    dataframes = load_voice_data()
    if dataframes is None:
        print("⚠️  SKIP [process_raw_voice_data]: Archivos parquet de voz no encontrados.")
        print("   Causa probable: el período de extracción no generó datos (SQL devolvió 0 filas).")
        print("   El pipeline continúa — no hay nuevos registros raw de voz para procesar.")
        return

    try:
        processed = process_raw_voice_data(dataframes)
    except ValueError as exc:
        if _is_empty_data_error(exc):
            print(f"⚠️  SKIP [process_raw_voice_data]: {exc}")
            print("   No hay datos procesables en este período. Omitiendo almacenamiento.")
            return
        raise

    try:
        validate_raw_voice_data(processed)
    except ValueError as exc:
        if _is_empty_data_error(exc):
            print(f"⚠️  SKIP [process_raw_voice_data]: {exc}")
            return
        raise

    print("Almacenando datos de voz raw en PostgreSQL...")
    postgres_handler.upsert_raw_voice_measurements(processed)
    print(f"✅ {len(processed):,} registros almacenados en voice_raw_measurements")


if __name__ == "__main__":
    main()
