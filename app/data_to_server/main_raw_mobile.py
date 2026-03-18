"""
main_raw_mobile.py
──────────────────
ETL de datos móviles crudos → mobile_raw_measurements (regulatorio / Grafana).

Diferencias respecto a main.py (PowerBI / analítico):
  - Sin filtro de SessionType: se almacenan todos los tipos de sesión.
    La vista grafana_mobile_geo_view filtra HTTP Post / HTTP Download.
  - Sin dropna de coordenadas ni ThroughputMbps: sesiones fallidas o sin
    GPS son datos regulatorios válidos (representan que la red no respondió).
  - ThroughputMbps se calcula igual; queda NULL en sesiones fallidas.

Extensibilidad:
  Cualquier análisis futuro (FTP, Video, etc.) solo requiere una nueva
  vista sobre mobile_raw_measurements, sin reprocesar histórico.
"""

import gc
import ipaddress
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
# Carga de datos — idéntica a main.py
# ---------------------------------------------------------------------------

def load_mobile_data():
    data_dir = '/opt/airflow/app/data'
    mobile_files = {
        'table1': f"{data_dir}/extract_datos_table1.parquet",
        'table2': f"{data_dir}/extract_datos_table2.parquet",
        'sense_nacional': f"{data_dir}/sense_nacional_v0.xlsx",
    }

    missing = [p for p in mobile_files.values() if not os.path.exists(p)]
    if missing:
        print(f"ERROR: Archivos no encontrados: {', '.join(missing)}")
        return None

    dataframes = {}
    for key in ['table1', 'table2']:
        try:
            print(f"Leyendo: {mobile_files[key]}")
            dataframes[key] = pd.read_parquet(mobile_files[key])
            print(f"  {key}: {len(dataframes[key]):,} registros")
        except Exception as e:
            print(f"Error cargando {mobile_files[key]}: {e}")
            return None

    try:
        print(f"Leyendo: {mobile_files['sense_nacional']}")
        dataframes['sense_nacional'] = pd.read_excel(mobile_files['sense_nacional'])
        print(f"  sense_nacional: {len(dataframes['sense_nacional']):,} registros")
    except Exception as e:
        print(f"Error cargando sense_nacional: {e}")
        return None

    return dataframes


# ---------------------------------------------------------------------------
# table1 — igual que main.py EXCEPTO: sin filtro de SessionType
# ---------------------------------------------------------------------------

def _process_table1_raw(df):
    """
    Normaliza SessionSummary sin filtrar por SessionType.
    main.py filtra isin(['HTTP Download', 'HTTP Post']). Aquí se conservan todos.
    """
    print("Procesando SessionSummary (raw — todos los SessionType)...")

    try:
        dup_cols = [c for c in [
            'DatasourceId', 'SessionType', 'StartRadioTechnology', 'IMSI', 'IMEI',
            'StartLatitude', 'StartLongitude', 'EndLatitude', 'EndLongitude',
            'StartTime', 'EndTime', 'EndRadioTechnology', 'Operator', 'SimOperator',
        ] if c in df.columns]

        if dup_cols:
            before = len(df)
            df = df.drop_duplicates(subset=dup_cols, keep='first')
            print(f"  Dedup table1: {before:,} → {len(df):,} ({before - len(df):,} eliminados)")

        df.loc[:, 'StartTime'] = pd.to_datetime(df['StartTime'], errors='coerce',
                                                format='%Y-%m-%d %H:%M:%S.%f')
        df.loc[:, 'EndTime'] = pd.to_datetime(df['EndTime'], errors='coerce',
                                              format='%Y-%m-%d %H:%M:%S.%f')

        for col in ['StartLatitude', 'StartLongitude', 'EndLatitude', 'EndLongitude']:
            if col in df.columns:
                df.loc[:, col] = df[col].astype(float)

        df.loc[:, 'IMSI'] = pd.to_numeric(df['IMSI'], errors='coerce').astype('Int64')
        df.loc[:, 'IMEI'] = pd.to_numeric(df['IMEI'], errors='coerce').astype('Int64')

        if 'LogfileId' in df.columns:
            df.loc[:, 'LogfileId'] = pd.to_numeric(df['LogfileId'], errors='coerce').astype('Int64')

        if 'IpAddress' in df.columns:
            def convertir_ip(ip):
                try:
                    return ipaddress.ip_address(ip)
                except Exception:
                    return None

            df.loc[:, 'IpAddress'] = df['IpAddress'].apply(convertir_ip)

        if 'SessionType' in df.columns:
            print(f"  SessionType distribución: {df['SessionType'].value_counts().to_dict()}")

        print(f"  table1 raw lista: {len(df):,} filas")
        return df

    except Exception as e:
        raise ValueError(f"Error procesando table1 raw: {e}")


# ---------------------------------------------------------------------------
# table2 — idéntico a main.py (no tiene filtros ni dropna)
# ---------------------------------------------------------------------------

def _process_table2(df):
    """Normaliza SessionSummaryData — lógica idéntica a main.py."""
    print("Procesando SessionSummaryData...")

    try:
        dup_cols = [c for c in [
            'DatasourceId', 'SessionType', 'StartDateTime', 'EndDateTime',
            'EndServiceBearer', 'EndDataRadioBearer', 'EndFileSize', 'EndServiceStatus',
        ] if c in df.columns]

        if dup_cols:
            before = len(df)
            df = df.drop_duplicates(subset=dup_cols, keep='first')
            print(f"  Dedup table2: {before:,} → {len(df):,}")

        datetime_cols = [
            'StartDateTime', 'EndDateTime', 'ErrorDateTime',
            'IPServiceSetupTimeMethodADateTime', 'IPServiceSetupTimeMethodBDateTime',
            'DataTransferTimeMethodADateTime', 'DataTransferTimeMethodBDateTime',
            'MeanDataRateMethodADateTime', 'MeanDataRateMethodBDateTime',
            'IPServiceAccessFailureMethodADateTime', 'IPServiceAccessFailureMethodBDateTime',
            'DataTransferCutoffMethodADateTime', 'DataTransferCutoffMethodBDateTime',
        ]
        for col in datetime_cols:
            if col in df.columns:
                df.loc[:, col] = pd.to_datetime(df[col], errors='coerce',
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
            'FixedDuration', 'IPServiceSetupTimeMethodAServiceSetupTime',
            'IPServiceSetupTimeMethodBServiceSetupTime',
            'DataTransferTimeMethodADuration', 'DataTransferTimeMethodBDuration',
            'TcpHandshakeTime', 'DnsHostNameResolutionTimeResolutionTime',
            'TimeSpentOnLte', 'TimeSpentOnNr', 'TimeOnMixed', 'TotalTime',
        ]
        for col in duration_cols:
            if col in df.columns:
                df.loc[:, col] = df[col].apply(convertir_a_duracion)

        float_cols = [
            'EndFileSize', 'ServiceAccessStartRssi', 'ServiceAccessStartRsrp',
            'ServiceAccessStartSinr', 'ServiceAccessStartRscp', 'ServiceAccessStartEcNo',
            'MeanDataRateMethodAThroughputKbps', 'MeanDataRateMethodBThroughputKbps',
            'ThroughputDownlinkAfter5sKbps', 'ThroughputDownlinkAfter10sKbps',
            'ThroughputDownlinkAfter15sKbps', 'ThroughputUplinkAfter5sKbps',
            'ThroughputUplinkAfter10sKbps', 'ThroughputUplinkAfter15sKbps',
            'ReceivedChunk1AggThroughput', 'ReceivedChunk2AggThroughput',
            'ReceivedChunk3AggThroughput', 'ReceivedChunk4AggThroughput',
            'ReceivedChunk5AggThroughput', 'ReceivedChunk6AggThroughput',
            'ReceivedChunk7AggThroughput', 'ReceivedChunk8AggThroughput',
            'ReceivedChunk9AggThroughput', 'ReceivedChunk10AggThroughput',
            'ReceivedChunk1Size', 'ReceivedChunk2Size', 'ReceivedChunk3Size',
            'ReceivedChunk4Size', 'ReceivedChunk5Size', 'ReceivedChunk6Size',
            'ReceivedChunk7Size', 'ReceivedChunk8Size', 'ReceivedChunk9Size',
            'ReceivedChunk10Size', 'ApplicationLayerThroughputDownlinkMax',
            'ApplicationLayerThroughputUplinkMax', 'DNSResolutionStartRssi',
            'DNSResolutionStartRsrp', 'DNSResolutionStartSinr', 'DNSResolutionStartRscp',
            'DNSResolutionStartEcNo', 'SessionEndRssi', 'SessionEndRsrp',
            'SessionEndSinr', 'SessionEndRscp', 'SessionEndEcNo',
            'ApplicationLayerThroughputDownlinkMean', 'ApplicationLayerThroughputUplinkMean',
            'CarrierAggregationCellList', 'AverageThroughputLteKbps',
        ]
        for col in float_cols:
            if col in df.columns:
                df.loc[:, col] = pd.to_numeric(df[col], errors='coerce').astype(float)

        int_cols = [
            'DatasourceId', 'SessionId', 'StartSampleId', 'EndSampleId',
            'ErrorSampleId', 'MultiRab', 'ErrorErrorCauseSubCause',
            'IsTimeBasedMeasurement', 'IPServiceSetupTimeMethodASampleId',
            'IPServiceSetupTimeMethodBSampleId', 'ServiceAccessStartRnceNodeB',
            'ServiceAccessStartSectorCell', 'ServiceAccessStartPciSC',
            'ServiceAccessStartLacTac', 'DataTransferTimeMethodASampleId',
            'DataTransferTimeMethodBSampleId', 'MeanDataRateMethodASampleId',
            'MeanDataRateMethodBSampleId', 'IPServiceAccessFailureMethodASampleId',
            'IPServiceAccessFailureMethodBSampleId', 'DataTransferCutoffMethodASampleId',
            'DataTransferCutoffMethodBSampleId', 'ReceivedChunk1AggDuration',
            'ReceivedChunk2AggDuration', 'ReceivedChunk3AggDuration',
            'ReceivedChunk4AggDuration', 'ReceivedChunk5AggDuration',
            'ReceivedChunk6AggDuration', 'ReceivedChunk7AggDuration',
            'ReceivedChunk8AggDuration', 'ReceivedChunk9AggDuration',
            'ReceivedChunk10AggDuration', 'SentChunk1Duration', 'SentChunk2Duration',
            'SentChunk3Duration', 'SentChunk4Duration', 'SentChunk5Duration',
            'SentChunk6Duration', 'SentChunk7Duration', 'SentChunk8Duration',
            'SentChunk9Duration', 'SentChunk10Duration', 'MaxEpsServingCellCount',
            'DNSResolutionStartRnceNodeB', 'DNSResolutionStartSectorCell',
            'DNSResolutionStartPciSC', 'DNSResolutionStartLacTac',
            'SessionEndRnceNodeB', 'SessionEndSectorCell', 'SessionEndPciSC',
            'SessionEndLacTac', 'CarrierAggregation', 'CarrierAggregationUplink',
            'KbyteCountLte', 'KbyteCountNr',
        ]
        for col in int_cols:
            if col in df.columns:
                df.loc[:, col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

        return df

    except Exception as e:
        raise ValueError(f"Error procesando table2: {e}")


# ---------------------------------------------------------------------------
# sense_nacional — idéntico a main.py
# ---------------------------------------------------------------------------

def _process_sense_nacional(df):
    print("Procesando sense_nacional...")
    try:
        df.loc[:, 'IMSI'] = pd.to_numeric(df['IMSI'], errors='coerce').astype('Int64')
        df.loc[:, 'IMEI'] = pd.to_numeric(df['IMEI'], errors='coerce').astype('Int64')
        return df
    except Exception as e:
        raise ValueError(f"Error procesando sense_nacional: {e}")


# ---------------------------------------------------------------------------
# Throughput — idéntico a main.py
# ---------------------------------------------------------------------------

def calculate_throughput(row):
    if (row['EndFileSize'] != 0 and
            row['EndServiceStatus'] == 'Succeeded' and
            pd.notna(row['DataTransferTimeMethodADuration'])):
        total_seconds = row['DataTransferTimeMethodADuration'].total_seconds()
        try:
            if total_seconds > 0:
                return (float(row['EndFileSize']) * 8) / total_seconds / 1_000_000
        except (ZeroDivisionError, TypeError):
            return None
    return None


# ---------------------------------------------------------------------------
# Merge raw — igual que main.py EXCEPTO: sin dropna final
# ---------------------------------------------------------------------------

def process_raw_mobile_data(dataframes, chunk_size=10_000):
    """
    Merge y normalización SIN dropna de coordenadas ni ThroughputMbps.
    Secuencia de merges idéntica a main.py.
    """
    print("🚀 INICIANDO PROCESO RAW MOBILE (sin filtrado de nulos)")
    print("=" * 70)

    df1 = dataframes['table1'].copy()
    df2 = dataframes['table2'].copy()
    dfsense = dataframes['sense_nacional'].copy()
    del dataframes
    gc.collect()

    print(f"Table1 (SessionSummary)     : {df1.shape}")
    print(f"Table2 (SessionSummaryData) : {df2.shape}  ← tabla más grande")
    print(f"sense_nacional              : {dfsense.shape}")

    try:
        # PASO 1: normalización
        print("\n📱 PASO 1: Normalizando tipos...")
        df1 = df1.rename(columns={'SessionIdOrCallIndex': 'SessionId',
                                  'SessionEndStatus': 'EndServiceStatus'})
        df2 = df2.rename(columns={'StartDateTime': 'StartTime',
                                  'EndDateTime': 'EndTime'})
        df1 = _process_table1_raw(df1)
        df2 = _process_table2(df2)
        dfsense = _process_sense_nacional(dfsense)

        if df1.empty or df2.empty:
            raise ValueError("❌ Una tabla de origen quedó vacía tras normalización")

        # PASO 2: merge en chunks df2 + df1 (idéntico a main.py)
        print("\n📊 PASO 2: Merge df2 + df1 en chunks...")
        merge_columns = ['DatasourceId', 'SessionId', 'SessionType',
                         'StartTime', 'EndTime', 'EndServiceStatus']
        processed_chunks = []
        num_chunks = len(df2) // chunk_size + (1 if len(df2) % chunk_size else 0)
        failed_chunks = 0

        for i in range(0, len(df2), chunk_size):
            chunk_num = i // chunk_size + 1
            chunk_df2 = df2.iloc[i: i + chunk_size].copy()
            try:
                on_cols = [c for c in merge_columns
                           if c in df1.columns and c in chunk_df2.columns]
                chunk_merged = df1.merge(chunk_df2, how='right', on=on_cols,
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
                del chunk_df2
                gc.collect()

        if not processed_chunks:
            raise ValueError("❌ Ningún chunk procesado exitosamente")

        df2_1_merged = pd.concat(processed_chunks, ignore_index=True)
        del processed_chunks, df1, df2
        gc.collect()
        print(f"✅ Tras merge 1: {len(df2_1_merged):,} filas")

        # PASO 3: merge con sense_nacional
        print("\n📱 PASO 3: Merge con sense_nacional (IMSI + IMEI)...")
        dataset_final = df2_1_merged.merge(dfsense, on=['IMSI', 'IMEI'],
                                           how='left', suffixes=('', '_dispositivo'))
        del df2_1_merged, dfsense
        gc.collect()
        print(f"✅ Tras merge 2 (final): {len(dataset_final):,} filas")

        # PASO 4: throughput (NULL = sesión fallida, es dato válido)
        print("\n⚡ PASO 4: Calculando ThroughputMbps...")
        dataset_final['ThroughputMbps'] = dataset_final.apply(calculate_throughput, axis=1)

        # Resumen SIN dropna
        total = len(dataset_final)
        sin_coords = (dataset_final[['EndLatitude', 'EndLongitude']]
                      .isna().any(axis=1).sum()
                      if 'EndLatitude' in dataset_final.columns else 0)
        sin_throughput = dataset_final['ThroughputMbps'].isna().sum()
        con_device = (dataset_final['Device'].notna().sum()
                      if 'Device' in dataset_final.columns else 0)

        print(f"\n📊 RESUMEN DATASET RAW MÓVIL:")
        print(f"   Total filas                   : {total:,}")
        print(f"   Sin coordenadas (conservadas) : {sin_coords:,} ({sin_coords / total * 100:.1f}%)")
        print(f"   Sin throughput  (conservados) : {sin_throughput:,} ({sin_throughput / total * 100:.1f}%)")
        print(f"   Con Device enriquecido        : {con_device:,} ({con_device / total * 100:.1f}%)")
        print("=" * 70)

        return dataset_final

    except Exception as e:
        gc.collect()
        raise


# ---------------------------------------------------------------------------
# Validación mínima
# ---------------------------------------------------------------------------

def validate_raw_mobile_data(df):
    if df.empty:
        raise ValueError("Dataset raw móvil vacío")
    if 'DatasourceId' not in df.columns or df['DatasourceId'].isnull().all():
        raise ValueError("DatasourceId ausente o completamente nulo")
    print(f"✅ Validación raw OK: {len(df):,} filas × {len(df.columns)} columnas")
    return True


# ---------------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------------

def main():
    print("Iniciando ETL de datos móviles CRUDOS...")

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

    dataframes = load_mobile_data()
    if dataframes is None:
        raise RuntimeError("No se pudieron cargar los datos móviles")

    processed = process_raw_mobile_data(dataframes)
    validate_raw_mobile_data(processed)

    print("Almacenando datos móviles raw en PostgreSQL...")
    postgres_handler.upsert_raw_measurements(processed)
    print(f"✅ {len(processed):,} registros almacenados en mobile_raw_measurements")


if __name__ == "__main__":
    main()
