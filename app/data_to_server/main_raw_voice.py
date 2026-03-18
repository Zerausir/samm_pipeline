"""
main_raw_voice.py
─────────────────
ETL de datos de voz crudos → voice_raw_measurements (regulatorio / Grafana).

Diferencias respecto a main_voice.py (PowerBI / analítico):
  - Sin filtro de SessionType en table1: se conservan todos (MO, MT, etc.).
    La vista grafana_voice_geo_view filtra CallDirection = 'MO'.
  - Sin dropna de coordenadas en table4: filas sin GPS se conservan.
  - Sin dropna final de coordenadas.

Extensibilidad:
  Llamadas MT u otros análisis futuros solo requieren una nueva vista
  sobre voice_raw_measurements, sin reprocesar histórico.
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
# Carga de datos — idéntica a main_voice.py
# ---------------------------------------------------------------------------

def load_voice_data():
    data_dir = '/opt/airflow/app/data'
    voice_files = {
        'table1': f"{data_dir}/extract_voz_table1.parquet",
        'table3': f"{data_dir}/extract_voz_table3.parquet",
        'table4': f"{data_dir}/extract_voz_table4.parquet",
        'sense_nacional': f"{data_dir}/sense_nacional_v0.xlsx",
    }

    missing = [p for p in voice_files.values() if not os.path.exists(p)]
    if missing:
        print(f"ERROR: Archivos no encontrados: {', '.join(missing)}")
        return None

    dataframes = {}
    for key in ['table1', 'table3', 'table4']:
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
# table1 — igual que main_voice.py EXCEPTO: sin filtro SessionType == 'Voice MO'
# ---------------------------------------------------------------------------

def _process_table1_raw(df):
    """
    Normaliza SessionSummary sin filtrar por SessionType.
    main_voice.py filtra SessionType == 'Voice MO'. Aquí se conservan todos.
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

    print(f"  table1 raw lista: {len(df):,} filas (sin filtro SessionType)")
    return df


# ---------------------------------------------------------------------------
# table3 — idéntico a main_voice.py (no tiene filtros ni dropna)
# ---------------------------------------------------------------------------

def _process_table3(df):
    """Normaliza SessionSummaryVoice — lógica idéntica a main_voice.py."""
    print("Procesando SessionSummaryVoice...")

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
        'AqmSessionEndAqmCallQuality', 'AqmSessionEndAqmCallQualityDownlink',
        'AqmSessionEndAqmCallQualityUplink', 'AqmAlgorithmDownlink',
        'AqmAlgorithmUplink', 'SpeechCodecs', 'SpeechPathDelayOneWay',
        'SilentCall', 'SpeechInterruptionTimeDownlinkDuration',
        'RtpInterruptionTimeAudioInterruptionTime',
        'HandoverSpeechInterruptionDownlinkDuration', 'CallSetupOffHookTime',
        'CallAttemptAccessNetworkInfo', 'CallSetupAccessNetworkInfo',
        'CallSetupDomain', 'CallSetupUserPerceivedCallSetupTime',
        'CallEstablishedUserCallEstablishedTime', 'CallEstablishedCallAnswerDelay',
        'BearerTechnology',
    ]
    existing = [c for c in required_columns if c in df.columns]
    df = df[existing].copy()

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

    for col in ['AqmSessionEndAqmCallQuality', 'AqmSessionEndAqmCallQualityDownlink']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)

    for col in ['DatasourceId', 'DialStartPhoneNumber', 'DialEndSampleId',
                'AqmSessionEndOtherPartyPhoneNumber', 'AqmSessionEndPhoneNumber']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    print(f"  table3 lista: {len(df):,} filas")
    return df


# ---------------------------------------------------------------------------
# table4 — igual que main_voice.py EXCEPTO: sin dropna de coordenadas
# ---------------------------------------------------------------------------

def _process_table4_raw(df):
    """
    Normaliza SessionVoiceQuality sin eliminar coordenadas nulas.
    main_voice.py hace dropna(['EndLatitude','EndLongitude']). Aquí se conservan.
    """
    print("Procesando SessionVoiceQuality (raw — conserva coordenadas nulas)...")

    required_columns = [
        'DatasourceId', 'CallIndex', 'SentenceIndex',
        'StartDateTime', 'EndDateTime', 'EndLatitude', 'EndLongitude',
        'AqmScoreAny', 'AqmScoreDownlink', 'AqmScoreUplink',
        'SpeechCodec', 'AqmAlgorithmDownlink', 'AqmAlgorithmUplink',
    ]
    existing = [c for c in required_columns if c in df.columns]
    df = df[existing].copy()

    dup_cols = [c for c in [
        'DatasourceId', 'StartDateTime', 'EndDateTime',
        'EndLatitude', 'EndLongitude', 'EndRadioTechnology',
    ] if c in df.columns]
    if dup_cols:
        before = len(df)
        df = df.drop_duplicates(subset=dup_cols, keep='first')
        print(f"  Dedup table4: {before:,} → {len(df):,}")

    for col in ['DatasourceId']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    for col in ['StartDateTime', 'EndDateTime']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce',
                                     format='%Y-%m-%d %H:%M:%S.%f')
    for col in ['EndLatitude', 'EndLongitude']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
    for col in ['AqmScoreAny', 'AqmScoreDownlink', 'AqmScoreUplink']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)

    null_coords = (df[['EndLatitude', 'EndLongitude']].isna().any(axis=1).sum()
                   if 'EndLatitude' in df.columns else 0)
    print(f"  table4 raw lista: {len(df):,} filas "
          f"(coords nulas conservadas: {null_coords:,})")
    return df


# ---------------------------------------------------------------------------
# sense_nacional — idéntico a main_voice.py
# ---------------------------------------------------------------------------

def _process_sense_nacional(df):
    print("Procesando sense_nacional...")
    df['IMSI'] = pd.to_numeric(df['IMSI'], errors='coerce').astype('Int64')
    df['IMEI'] = pd.to_numeric(df['IMEI'], errors='coerce').astype('Int64')
    return df


# ---------------------------------------------------------------------------
# Merge raw — igual que main_voice.py EXCEPTO: sin dropna final
# ---------------------------------------------------------------------------

def process_raw_voice_data(dataframes, chunk_size=8_000):
    """
    Merge y normalización SIN dropna de coordenadas ni filtro de CallDirection.
    Secuencia de merges idéntica a main_voice.py.
    """
    print("🚀 INICIANDO PROCESO RAW VOICE (sin filtrado de nulos)")
    print("=" * 70)

    df1 = dataframes['table1'].copy()
    df2 = dataframes['table3'].copy()
    df3 = dataframes['table4'].copy()
    df4 = dataframes['sense_nacional'].copy()
    del dataframes
    gc.collect()

    print(f"Table1 (SessionSummary)      : {df1.shape}")
    print(f"Table3 (SessionSummaryVoice) : {df2.shape}")
    print(f"Table4 (SessionVoiceQuality) : {df3.shape}  ← tabla más grande")
    print(f"sense_nacional               : {df4.shape}")

    try:
        # PASO 1: normalización
        print("\n📞 PASO 1: Normalizando tipos...")
        df1 = _process_table1_raw(df1)
        df2 = _process_table3(df2)
        df3 = _process_table4_raw(df3)
        df4 = _process_sense_nacional(df4)

        if df3.empty or df2.empty:
            raise ValueError("❌ Una tabla de origen quedó vacía tras normalización")

        # PASO 2: merge en chunks df3 + df2 (idéntico a main_voice.py)
        # main_voice.py: chunk_df3.merge(df2, on=['DatasourceId','CallIndex'], how='left')
        print("\n📊 PASO 2: Merge df3 + df2 en chunks (VoiceQuality + SessionSummaryVoice)...")
        merge_cols_32 = ['DatasourceId', 'CallIndex']
        processed_chunks = []
        num_chunks = len(df3) // chunk_size + (1 if len(df3) % chunk_size else 0)
        failed_chunks = 0

        for i in range(0, len(df3), chunk_size):
            chunk_num = i // chunk_size + 1
            chunk_df3 = df3.iloc[i: i + chunk_size].copy()
            try:
                on_cols = [c for c in merge_cols_32
                           if c in chunk_df3.columns and c in df2.columns]
                chunk_merged = chunk_df3.merge(df2, on=on_cols, how='left',
                                               suffixes=('', '_df2'))
                if len(chunk_merged) > 0:
                    processed_chunks.append(chunk_merged)
                print(f"  Chunk {chunk_num}/{num_chunks}: {len(chunk_merged):,} filas")
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

        df3_2_merged = pd.concat(processed_chunks, ignore_index=True)
        del processed_chunks, df3, df2
        gc.collect()
        print(f"✅ Tras merge 1: {len(df3_2_merged):,} filas")

        # PASO 3: merge resultado + df1 (idéntico a main_voice.py)
        print("\n📞 PASO 3: Merge resultado + SessionSummary (DatasourceId + CallIndex)...")
        if 'SessionIdOrCallIndex' in df1.columns:
            df1 = df1.rename(columns={'SessionIdOrCallIndex': 'CallIndex'})
            merge_cols_12 = ['DatasourceId', 'CallIndex']
        else:
            merge_cols_12 = ['DatasourceId']

        df3_2_1_merged = df3_2_merged.merge(df1, on=merge_cols_12, how='left',
                                            suffixes=('', '_df1'))
        del df3_2_merged, df1
        gc.collect()
        print(f"✅ Tras merge 2: {len(df3_2_1_merged):,} filas")

        # PASO 4: merge con sense_nacional
        print("\n📱 PASO 4: Merge con sense_nacional (IMSI + IMEI)...")
        dataset_final = df3_2_1_merged.merge(df4, on=['IMSI', 'IMEI'],
                                             how='left', suffixes=('', '_df4'))
        del df3_2_1_merged, df4
        gc.collect()
        print(f"✅ Tras merge 3 (final): {len(dataset_final):,} filas")

        # Resumen SIN dropna
        total = len(dataset_final)
        sin_coords = (dataset_final[['EndLatitude', 'EndLongitude']]
                      .isna().any(axis=1).sum()
                      if 'EndLatitude' in dataset_final.columns else 0)
        con_device = (dataset_final['Device'].notna().sum()
                      if 'Device' in dataset_final.columns else 0)

        print(f"\n📊 RESUMEN DATASET RAW VOZ:")
        print(f"   Total filas                   : {total:,}")
        print(f"   Sin coordenadas (conservadas) : {sin_coords:,} ({sin_coords / total * 100:.1f}%)")
        print(f"   Con Device enriquecido        : {con_device:,} ({con_device / total * 100:.1f}%)")
        print("=" * 70)

        return dataset_final

    except Exception as e:
        gc.collect()
        raise


# ---------------------------------------------------------------------------
# Validación mínima
# ---------------------------------------------------------------------------

def validate_raw_voice_data(df):
    if df.empty:
        raise ValueError("Dataset raw de voz vacío")
    if 'DatasourceId' not in df.columns or df['DatasourceId'].isnull().all():
        raise ValueError("DatasourceId ausente o completamente nulo")
    print(f"✅ Validación raw voz OK: {len(df):,} filas × {len(df.columns)} columnas")
    return True


# ---------------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------------

def main():
    print("Iniciando ETL de datos de voz CRUDOS...")

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
        raise RuntimeError("No se pudieron cargar los datos de voz")

    processed = process_raw_voice_data(dataframes)
    validate_raw_voice_data(processed)

    print("Almacenando datos de voz raw en PostgreSQL...")
    postgres_handler.upsert_raw_voice_measurements(processed)
    print(f"✅ {len(processed):,} registros almacenados en voice_raw_measurements")


if __name__ == "__main__":
    main()
