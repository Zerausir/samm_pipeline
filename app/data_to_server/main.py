import os
import pandas as pd
import geopandas as gpd
import datetime
from datetime import timedelta, datetime
import ipaddress
import gc
from app.utils.postgres_handler import get_postgres_handler


def get_env_var(var_name, default=None):
    """Get environment variable, optionally loading from .env file"""
    value = os.environ.get(var_name)
    if value is None:
        # Only load from .env if the variable isn't already set
        from dotenv import load_dotenv
        load_dotenv()
        value = os.environ.get(var_name)
    return value if value is not None else default


def load_mobile_data():
    """
    Load mobile data from parquet files and Excel file and return as separate dataframes
    """
    # Define data directory
    data_dir = '/opt/airflow/app/data'  # Path in container

    # Define file paths
    mobile_files = {
        'table1': f"{data_dir}/extract_datos_table1.parquet",
        'table2': f"{data_dir}/extract_datos_table2.parquet",
        'sense_nacional': f"{data_dir}/sense_nacional_v0.xlsx"
    }

    # Verify files exist
    missing_files = []
    for table_name, file_path in mobile_files.items():
        if not os.path.exists(file_path):
            missing_files.append(file_path)

    if missing_files:
        print(f"ERROR: Mobile data files not found: {', '.join(missing_files)}")
        print("Please run the data extraction script on your local PC first.")
        return None

    # Load data from files
    dataframes = {}

    # Load parquet files
    for table_name in ['table1', 'table2']:
        try:
            print(f"Reading file: {mobile_files[table_name]}")
            df = pd.read_parquet(mobile_files[table_name])
            dataframes[table_name] = df
            print(f"Loaded {table_name}: {len(df)} records")
        except Exception as e:
            print(f"Error loading {mobile_files[table_name]}: {e}")
            return None

    # Load Excel file for sense_nacional
    try:
        print(f"Reading file: {mobile_files['sense_nacional']}")
        df = pd.read_excel(mobile_files['sense_nacional'])
        dataframes['sense_nacional'] = df
        print(f"Loaded sense_nacional: {len(df)} records")
    except Exception as e:
        print(f"Error loading {mobile_files['sense_nacional']}: {e}")
        return None

    return dataframes


def _process_table1_session_summary(df):
    """Process SessionSummary table (table1) exactly like data_analysis_code.ipynb"""
    print("Processing SessionSummary data...")

    # OPTIMIZACIÓN 3: MANEJO DE ERRORES EN PROCESAMIENTO
    try:
        # FILTER FIRST: Only keep HTTP Download and HTTP Post sessions to reduce data volume
        initial_rows = len(df)
        if 'SessionType' in df.columns:
            df = df[df['SessionType'].isin(['HTTP Download', 'HTTP Post'])].copy()
            filtered_rows = len(df)
            print(
                f"Filtered for HTTP Download/Post sessions: {initial_rows:,} → {filtered_rows:,} records (-{initial_rows - filtered_rows:,})")

            # Show breakdown by session type
            if len(df) > 0:
                session_counts = df['SessionType'].value_counts()
                print(f"   - HTTP Download: {session_counts.get('HTTP Download', 0):,} records")
                print(f"   - HTTP Post: {session_counts.get('HTTP Post', 0):,} records")
        else:
            print("Warning: SessionType column not found, processing all records")

        if df.empty:
            print("Warning: No HTTP Download/Post sessions found after filtering!")
            return df

        # REMOVE DUPLICATES BEFORE DATA TYPE CONVERSION
        duplicate_columns = [
            'DatasourceId', 'SessionType', 'StartRadioTechnology', 'IMSI', 'IMEI',
            'StartLatitude', 'StartLongitude', 'EndLatitude', 'EndLongitude',
            'StartTime', 'EndTime', 'EndRadioTechnology', 'Operator', 'SimOperator'
        ]

        # Only use columns that exist in the dataframe
        existing_duplicate_columns = [col for col in duplicate_columns if col in df.columns]

        if existing_duplicate_columns:
            before_dedup = len(df)
            df = df.drop_duplicates(subset=existing_duplicate_columns, keep='first')
            after_dedup = len(df)
            duplicates_removed = before_dedup - after_dedup
            print(
                f"Removed duplicates from table1: {before_dedup:,} → {after_dedup:,} records (-{duplicates_removed:,} duplicates)")

            if duplicates_removed > 0:
                print(f"   - Duplicate detection based on columns: {existing_duplicate_columns}")
        else:
            print("Warning: No duplicate detection columns found in table1")

        # Convert datetime columns using .loc to avoid warnings
        df.loc[:, 'StartTime'] = pd.to_datetime(df['StartTime'], errors='coerce', format='%Y-%m-%d %H:%M:%S.%f')
        df.loc[:, 'EndTime'] = pd.to_datetime(df['EndTime'], errors='coerce', format='%Y-%m-%d %H:%M:%S.%f')

        # Convert coordinate columns to float using .loc
        df.loc[:, 'StartLatitude'] = df['StartLatitude'].astype(float)
        df.loc[:, 'StartLongitude'] = df['StartLongitude'].astype(float)
        df.loc[:, 'EndLatitude'] = df['EndLatitude'].astype(float)
        df.loc[:, 'EndLongitude'] = df['EndLongitude'].astype(float)

        # Convert ID columns to Int64 - only process columns that exist
        df.loc[:, 'IMSI'] = pd.to_numeric(df['IMSI']).astype('Int64')
        df.loc[:, 'IMEI'] = pd.to_numeric(df['IMEI']).astype('Int64')

        # Only process LogfileId if it exists
        if 'LogfileId' in df.columns:
            df.loc[:, 'LogfileId'] = pd.to_numeric(df['LogfileId']).astype('Int64')

        # Process IP addresses only if column exists
        if 'IpAddress' in df.columns:
            def convertir_ip(ip):
                try:
                    return ipaddress.ip_address(ip)
                except:
                    return None

            df.loc[:, 'IpAddress'] = df['IpAddress'].apply(convertir_ip)

        return df

    except Exception as e:
        print(f"❌ Error processing table1: {e}")
        raise ValueError(f"Failed to process SessionSummary table: {e}")


def _process_table2_session_summary_data(df):
    """Process SessionSummaryData table (table2) exactly like data_analysis_code.ipynb"""
    print("Processing SessionSummaryData data...")

    # OPTIMIZACIÓN 3: MANEJO DE ERRORES EN PROCESAMIENTO
    try:
        # REMOVE DUPLICATES BEFORE DATA TYPE CONVERSION
        duplicate_columns = [
            'DatasourceId', 'SessionType', 'StartDateTime', 'EndDateTime',
            'EndServiceBearer', 'EndDataRadioBearer', 'EndFileSize', 'EndServiceStatus'
        ]

        # Only use columns that exist in the dataframe
        existing_duplicate_columns = [col for col in duplicate_columns if col in df.columns]

        if existing_duplicate_columns:
            before_dedup = len(df)
            df = df.drop_duplicates(subset=existing_duplicate_columns, keep='first')
            after_dedup = len(df)
            duplicates_removed = before_dedup - after_dedup
            print(
                f"Removed duplicates from table2: {before_dedup:,} → {after_dedup:,} records (-{duplicates_removed:,} duplicates)")

            if duplicates_removed > 0:
                print(f"   - Duplicate detection based on columns: {existing_duplicate_columns}")
        else:
            print("Warning: No duplicate detection columns found in table2")

        # Convert datetime columns using .loc
        datetime_columns = [
            'StartDateTime', 'EndDateTime', 'ErrorDateTime',
            'IPServiceSetupTimeMethodADateTime', 'IPServiceSetupTimeMethodBDateTime',
            'DataTransferTimeMethodADateTime', 'DataTransferTimeMethodBDateTime',
            'MeanDataRateMethodADateTime', 'MeanDataRateMethodBDateTime',
            'IPServiceAccessFailureMethodADateTime', 'IPServiceAccessFailureMethodBDateTime',
            'DataTransferCutoffMethodADateTime', 'DataTransferCutoffMethodBDateTime'
        ]
        for col in datetime_columns:
            if col in df.columns:
                df.loc[:, col] = pd.to_datetime(df[col], errors='coerce', format='%Y-%m-%d %H:%M:%S.%f')

        # Function to convert to timedelta - unified approach for all duration columns
        def convertir_a_duracion(valor):
            try:
                valor = str(valor).strip()[:15]  # Limit to 15 chars
                t = datetime.strptime(valor, "%H:%M:%S.%f")
                return timedelta(hours=t.hour, minutes=t.minute, seconds=t.second, microseconds=t.microsecond)
            except:
                return None

        # Apply conversion to ALL duration columns using timedelta
        duration_columns = [
            'FixedDuration', 'IPServiceSetupTimeMethodAServiceSetupTime', 'IPServiceSetupTimeMethodBServiceSetupTime',
            'DataTransferTimeMethodADuration', 'DataTransferTimeMethodBDuration',
            'TcpHandshakeTime', 'DnsHostNameResolutionTimeResolutionTime',
            'TimeSpentOnLte', 'TimeSpentOnNr', 'TimeOnMixed', 'TotalTime'
        ]
        for col in duration_columns:
            if col in df.columns:
                df.loc[:, col] = df[col].apply(convertir_a_duracion)

        # Convert float columns using .loc
        float_columns = [
            'EndFileSize', 'ServiceAccessStartRssi', 'ServiceAccessStartRsrp',
            'ServiceAccessStartSinr', 'ServiceAccessStartRscp',
            'ServiceAccessStartEcNo', 'MeanDataRateMethodAThroughputKbps',
            'MeanDataRateMethodBThroughputKbps', 'ThroughputDownlinkAfter5sKbps',
            'ThroughputDownlinkAfter10sKbps', 'ThroughputDownlinkAfter15sKbps',
            'ThroughputUplinkAfter5sKbps', 'ThroughputUplinkAfter10sKbps',
            'ThroughputUplinkAfter15sKbps', 'ReceivedChunk1AggThroughput',
            'ReceivedChunk2AggThroughput', 'ReceivedChunk3AggThroughput',
            'ReceivedChunk4AggThroughput', 'ReceivedChunk5AggThroughput',
            'ReceivedChunk6AggThroughput', 'ReceivedChunk7AggThroughput',
            'ReceivedChunk8AggThroughput', 'ReceivedChunk9AggThroughput',
            'ReceivedChunk10AggThroughput', 'ReceivedChunk1Size',
            'ReceivedChunk2Size', 'ReceivedChunk3Size', 'ReceivedChunk4Size',
            'ReceivedChunk5Size', 'ReceivedChunk6Size', 'ReceivedChunk7Size',
            'ReceivedChunk8Size', 'ReceivedChunk9Size', 'ReceivedChunk10Size',
            'ApplicationLayerThroughputDownlinkMax', 'ApplicationLayerThroughputUplinkMax',
            'DNSResolutionStartRssi', 'DNSResolutionStartRsrp', 'DNSResolutionStartSinr',
            'DNSResolutionStartRscp', 'DNSResolutionStartEcNo', 'SessionEndRssi',
            'SessionEndRsrp', 'SessionEndSinr', 'SessionEndRscp', 'SessionEndEcNo',
            'ApplicationLayerThroughputDownlinkMean', 'ApplicationLayerThroughputUplinkMean',
            'CarrierAggregationCellList', 'AverageThroughputLteKbps'
        ]
        for col in float_columns:
            if col in df.columns:
                df.loc[:, col] = pd.to_numeric(df[col], errors='coerce').astype(float)

        # Convert int columns to Int64 using .loc
        int_columns = [
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
            'ReceivedChunk10AggDuration', 'SentChunk1Duration',
            'SentChunk2Duration', 'SentChunk3Duration', 'SentChunk4Duration',
            'SentChunk5Duration', 'SentChunk6Duration', 'SentChunk7Duration',
            'SentChunk8Duration', 'SentChunk9Duration', 'SentChunk10Duration',
            'MaxEpsServingCellCount', 'DNSResolutionStartRnceNodeB',
            'DNSResolutionStartSectorCell', 'DNSResolutionStartPciSC',
            'DNSResolutionStartLacTac', 'SessionEndRnceNodeB', 'SessionEndSectorCell',
            'SessionEndPciSC', 'SessionEndLacTac', 'CarrierAggregation',
            'CarrierAggregationUplink', 'KbyteCountLte', 'KbyteCountNr'
        ]
        for col in int_columns:
            if col in df.columns:
                df.loc[:, col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

        return df

    except Exception as e:
        print(f"❌ Error processing table2: {e}")
        raise ValueError(f"Failed to process SessionSummaryData table: {e}")


def _process_sense_nacional(df):
    """Process sense_nacional Excel file exactly like df4 in CODIGOS_UNIDOS.ipynb"""
    print("Processing sense_nacional data...")

    # OPTIMIZACIÓN 3: MANEJO DE ERRORES EN PROCESAMIENTO
    try:
        # Convert IMSI and IMEI to Int64 using .loc
        df.loc[:, 'IMSI'] = pd.to_numeric(df['IMSI']).astype('Int64')
        df.loc[:, 'IMEI'] = pd.to_numeric(df['IMEI']).astype('Int64')

        return df

    except Exception as e:
        print(f"❌ Error processing sense_nacional: {e}")
        raise ValueError(f"Failed to process sense_nacional table: {e}")


def calculate_throughput(row):
    """Calculate throughput using timedelta - more robust and semantically correct"""
    if (row['EndFileSize'] != 0 and
            row['EndServiceStatus'] == 'Succeeded' and
            pd.notna(row['DataTransferTimeMethodADuration'])):

        # Use total_seconds() from timedelta
        total_seconds = row['DataTransferTimeMethodADuration'].total_seconds()

        try:
            if total_seconds > 0:
                return (float(row['EndFileSize']) * 8) / total_seconds / 1000 / 1000
        except (ZeroDivisionError, TypeError):
            return None
    return None


def process_mobile_data(dataframes, chunk_size=10000):
    """
    Process and merge mobile dataframes starting with the largest table (df2)
    Using chunks for the largest merge (df2 + df1)
    Following the same pattern as process_voice_data_chunked
    """
    print("🚀 INICIANDO PROCESO DE MERGE COMPLETO (CHUNKED) - ORDEN CORRECTO")
    print("=" * 70)

    # Extract individual dataframes
    df1 = dataframes['table1'].copy()  # cdr_SessionSummary
    df2 = dataframes['table2'].copy()  # cdr_SessionSummaryData (LA MÁS GRANDE)
    dfsense = dataframes['sense_nacional'].copy()  # sense_nacional

    # OPTIMIZACIÓN 1: LIBERAR MEMORIA INMEDIATAMENTE
    del dataframes  # Liberar dataframes originales
    gc.collect()
    print("✅ Memoria de dataframes originales liberada")

    print(f"Table1 (SessionSummary) shape: {df1.shape}")
    print(f"Table2 (SessionSummaryData) shape: {df2.shape} (LA MÁS GRANDE)")
    print(f"sense_nacional shape: {dfsense.shape}")
    print(f"Chunk size: {chunk_size:,}")

    try:
        # 1. DATA PROCESSING - Apply transformations
        print("\n📱 PASO 1: PROCESANDO DATOS...")

        # Rename columns exactly like the original notebook BEFORE processing
        df1 = df1.rename(columns={'SessionIdOrCallIndex': 'SessionId', 'SessionEndStatus': 'EndServiceStatus'})
        df2 = df2.rename(columns={'StartDateTime': 'StartTime', 'EndDateTime': 'EndTime'})

        # Process each dataframe
        df1 = _process_table1_session_summary(df1)
        df2 = _process_table2_session_summary_data(df2)
        dfsense = _process_sense_nacional(dfsense)

        # OPTIMIZACIÓN 2: VALIDAR QUE TENEMOS DATOS PARA HACER MERGE
        if df1.empty:
            raise ValueError("❌ CRITICAL: Table1 (SessionSummary) is empty after processing")
        if df2.empty:
            raise ValueError("❌ CRITICAL: Table2 (SessionSummaryData) is empty after processing")
        if dfsense.empty:
            raise ValueError("❌ CRITICAL: sense_nacional is empty after processing")

        print("✅ Todas las tablas contienen datos después del procesamiento")

        # 2. MERGE 1 EN CHUNKS: df2 (SessionSummaryData) + df1 (SessionSummary)
        print("\n📊 PASO 2: MERGE df2 + df1 EN CHUNKS (SessionSummaryData + SessionSummary)")
        print("-" * 50)
        print(f"Merging on: DatasourceId, SessionId, SessionType, StartTime, EndTime, EndServiceStatus")

        processed_chunks = []
        num_chunks = len(df2) // chunk_size + (1 if len(df2) % chunk_size > 0 else 0)
        failed_chunks = 0

        print(f"Processing {len(df2):,} records in {num_chunks} chunks of {chunk_size:,}")

        # Merge columns for df2 + df1
        merge_columns = ['DatasourceId', 'SessionId', 'SessionType', 'StartTime', 'EndTime', 'EndServiceStatus']

        for i in range(0, len(df2), chunk_size):
            chunk_end = min(i + chunk_size, len(df2))
            chunk_num = i // chunk_size + 1

            print(f"Processing chunk {chunk_num}/{num_chunks}: rows {i:,} to {chunk_end:,}")

            # OPTIMIZACIÓN 3: MANEJO DE ERRORES EN CHUNKS
            try:
                # Get chunk from df2
                chunk_df2 = df2.iloc[i:chunk_end].copy()

                # Merge chunk with df1 (using 'right' join like original code)
                chunk_merged = df1.merge(chunk_df2, how='right', on=merge_columns, suffixes=('', '_df2'))

                print(
                    f"  Chunk {chunk_num}: df1 ({len(df1)}) + df2 chunk ({len(chunk_df2)}) = {len(chunk_merged)} records")

                if len(chunk_merged) > 0:
                    processed_chunks.append(chunk_merged)
                else:
                    print(f"⚠️  WARNING: Chunk {chunk_num} resulted in 0 records after merge")

                # Clear chunk memory
                del chunk_df2, chunk_merged
                gc.collect()

            except Exception as e:
                failed_chunks += 1
                print(f"❌ ERROR processing chunk {chunk_num}: {e}")

                # Si fallan demasiados chunks, abortar
                if failed_chunks > num_chunks * 0.1:  # Si falla más del 10% de chunks
                    raise ValueError(f"❌ CRITICAL: Too many chunk failures ({failed_chunks}/{num_chunks}). Aborting.")

                print(f"⚠️  Continuing with remaining chunks ({failed_chunks} failures so far)")
                continue

        # Combine all chunks from MERGE 1
        print(f"\n🔄 COMBINANDO {len(processed_chunks)} CHUNKS DEL MERGE 1...")
        if processed_chunks:
            df2_1_merged = pd.concat(processed_chunks, ignore_index=True)
            print(f"Combined dataset after MERGE 1: {len(df2_1_merged):,} records")

            # OPTIMIZACIÓN 2: VALIDAR MERGE 1
            if len(df2_1_merged) == 0:
                raise ValueError("❌ MERGE 1 FAILED: No matching records between SessionSummaryData and SessionSummary")

            print("✅ MERGE 1 exitoso")
        else:
            raise ValueError("❌ MERGE 1 FAILED: No chunks were processed successfully!")

        # Clear memory
        del processed_chunks, df1, df2
        gc.collect()

        # 3. MERGE 2 NORMAL: Resultado + dfsense (sense_nacional)
        print("\n📱 PASO 3: MERGE RESULTADO + dfsense (+ sense_nacional) - NORMAL")
        print("-" * 50)
        print(f"Merging on: IMSI, IMEI")

        # Merge with sense_nacional (using 'left' join like original code)
        dataset_final = df2_1_merged.merge(dfsense, on=['IMSI', 'IMEI'], how='left', suffixes=('', '_dispositivo'))
        print(f"After MERGE 2 (final): {len(dataset_final):,} records")

        # OPTIMIZACIÓN 2: VALIDAR MERGE 2 (FINAL)
        if len(dataset_final) == 0:
            raise ValueError("❌ MERGE 2 FAILED: Final dataset is empty")

        print("✅ MERGE 2 exitoso")

        # Clear memory
        del df2_1_merged, dfsense
        gc.collect()

        # 4. CALCULANDO THROUGHPUT - Como en el código original
        print("\n⚡ PASO 4: CALCULANDO THROUGHPUT...")
        print("-" * 50)

        # Apply throughput calculation exactly like the original
        dataset_final['ThroughputMbps'] = dataset_final.apply(calculate_throughput, axis=1)

        # 5. LIMPIEZA FINAL - Como en el código original
        print("\n🧹 LIMPIEZA FINAL DE COORDENADAS Y THROUGHPUT...")
        initial_rows = len(dataset_final)

        # Remove rows with NaN values in critical columns exactly like the original
        dataset_final = dataset_final.dropna(
            subset=['StartLatitude', 'StartLongitude', 'EndLatitude', 'EndLongitude', 'ThroughputMbps'])
        final_rows = len(dataset_final)
        print(f"Filtrado de nulos: {initial_rows:,} → {final_rows:,} records (-{initial_rows - final_rows:,})")

        # VALIDAR QUE AÚN TENEMOS DATOS DESPUÉS DE FILTRAR
        if final_rows == 0:
            raise ValueError("❌ CRITICAL: No records remaining after coordinate and throughput filtering!")

        print(f"\n🎯 RESUMEN FINAL DEL MERGE COMPLETO")
        print("=" * 70)
        print(f"📊 Dataset final: {len(dataset_final):,} filas × {len(dataset_final.columns)} columnas")

        # Data quality summary
        print(f"\n📋 ENRIQUECIMIENTO DE DATOS:")

        # Check device enrichment from sense_nacional
        if 'Device' in dataset_final.columns:
            dispositivos_enriquecidos = dataset_final['Device'].notna().sum()
            print(
                f"   📱 Registros con datos de dispositivo: {dispositivos_enriquecidos:,} ({(dispositivos_enriquecidos / len(dataset_final) * 100):.1f}%)")

        # Check throughput calculation
        registros_con_throughput = dataset_final['ThroughputMbps'].notna().sum()
        print(
            f"   ⚡ Registros con throughput calculado: {registros_con_throughput:,} ({(registros_con_throughput / len(dataset_final) * 100):.1f}%)")

        # Throughput statistics
        if registros_con_throughput > 0:
            print(f"   📈 Throughput promedio: {dataset_final['ThroughputMbps'].mean():.2f} Mbps")
            print(f"   📈 Throughput mediano: {dataset_final['ThroughputMbps'].median():.2f} Mbps")

        # Show sample duration values for verification
        if 'DataTransferTimeMethodADuration' in dataset_final.columns:
            sample_durations = dataset_final['DataTransferTimeMethodADuration'].dropna().head(3)
            if len(sample_durations) > 0:
                print(f"   🔍 Duraciones de muestra:")
                for i, duration in enumerate(sample_durations):
                    print(f"     {i + 1}. {duration} ({duration.total_seconds():.2f}s)")

        print(f"\n🚀 EL MERGE SE COMPLETÓ EXITOSAMENTE")
        print("📈 ESTRATEGIA DE CHUNKS APLICADA AL MERGE MÁS GRANDE (df2+df1)")
        print("✅ OPTIMIZACIONES IMPLEMENTADAS:")
        print("   - Gestión de memoria mejorada")
        print("   - Validación de integridad en cada merge")
        print("   - Manejo de errores robusto en chunks")
        print("   - Cálculo de throughput optimizado")
        print("=" * 70)

        return dataset_final

    except Exception as e:
        print(f"❌ Error in process_mobile_data_chunked: {e}")
        # Asegurar limpieza de memoria antes de abortar
        gc.collect()
        raise


def validate_mobile_data(df):
    """
    Validate processed mobile data before storage
    Enhanced validation to match the final dataset structure from the notebook
    """
    print("Validating mobile data...")

    # Basic validation checks
    if df.empty:
        raise ValueError("Processed dataframe is empty")

    print(f"Validating {len(df)} mobile records with {len(df.columns)} columns")

    # 1. REQUIRED COLUMNS CHECK (from notebook)
    required_columns = ['DatasourceId', 'ThroughputMbps', 'StartLatitude', 'StartLongitude',
                        'EndLatitude', 'EndLongitude']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # 2. THROUGHPUT VALIDATION (critical for the analysis) - Updated for timedelta
    print("Validating ThroughputMbps...")
    throughput_stats = df['ThroughputMbps'].describe()
    print(f"   📊 ThroughputMbps statistics:")
    print(f"     - Count: {throughput_stats['count']}")
    print(f"     - Mean: {throughput_stats['mean']:.2f} Mbps")
    print(f"     - Median: {df['ThroughputMbps'].median():.2f} Mbps")
    print(f"     - Min: {throughput_stats['min']:.2f} Mbps")
    print(f"     - Max: {throughput_stats['max']:.2f} Mbps")

    # Verify no null values in ThroughputMbps (as per filtering)
    null_throughput = df['ThroughputMbps'].isnull().sum()
    if null_throughput > 0:
        raise ValueError(f"Found {null_throughput} null values in ThroughputMbps after filtering")

    # Validate DataTransferTimeMethodADuration is timedelta
    if 'DataTransferTimeMethodADuration' in df.columns:
        duration_col = df['DataTransferTimeMethodADuration'].dropna()
        if len(duration_col) > 0:
            sample_duration = duration_col.iloc[0]
            if not isinstance(sample_duration, pd.Timedelta):
                print(f"   ⚠️  Warning: DataTransferTimeMethodADuration is {type(sample_duration)}, expected timedelta")
            else:
                print(f"   ✅ DataTransferTimeMethodADuration correctly converted to timedelta")
                print(f"     - Sample duration: {sample_duration}")
                print(f"     - Sample seconds: {sample_duration.total_seconds():.2f}s")

    # 3. COORDINATE VALIDATION (critical for geographic analysis)
    print("Validating coordinates...")
    coordinate_pairs = [
        ('StartLatitude', 'StartLongitude'),
        ('EndLatitude', 'EndLongitude')
    ]

    for lat_col, lon_col in coordinate_pairs:
        try:
            lat_numeric = pd.to_numeric(df[lat_col], errors='coerce')
            lon_numeric = pd.to_numeric(df[lon_col], errors='coerce')

            valid_coords = ((lat_numeric.between(-90, 90)) &
                            (lon_numeric.between(-180, 180))).sum()

            # As per notebook, these should have no nulls after filtering
            null_coords = df[[lat_col, lon_col]].isnull().any(axis=1).sum()
            if null_coords > 0:
                raise ValueError(f"Found {null_coords} null coordinates in {lat_col}/{lon_col} after filtering")

            print(f"   ✅ Valid coordinate pairs ({lat_col}, {lon_col}): {valid_coords}")
        except Exception as e:
            raise ValueError(f"Error validating coordinates ({lat_col}, {lon_col}): {e}")

    # 4. KEY FIELD VALIDATION
    print("Validating key fields...")

    # Check DatasourceId (should not be all null)
    if df['DatasourceId'].isnull().all():
        raise ValueError("All DatasourceId values are null")

    # Check IMEI coverage
    if 'IMEI' in df.columns:
        non_null_imei = df['IMEI'].notna().sum()
        print(f"   📱 Valid IMEI records: {non_null_imei}/{len(df)} ({(non_null_imei / len(df) * 100):.1f}%)")

    # Check Device coverage (from sense data merge)
    if 'Device' in df.columns:
        non_null_device = df['Device'].notna().sum()
        print(f"   📱 Records with device info: {non_null_device}/{len(df)} ({(non_null_device / len(df) * 100):.1f}%)")

    # 5. DATETIME VALIDATION
    print("Validating datetime fields...")
    for col in ['StartTime', 'EndTime']:
        if col in df.columns:
            valid_dates = df[col].notna().sum()
            if valid_dates > 0:
                try:
                    valid_datetime_series = df[col].dropna()
                    if len(valid_datetime_series) > 0:
                        date_range = f"{valid_datetime_series.min()} to {valid_datetime_series.max()}"
                        print(f"   📅 Column {col}: {len(valid_datetime_series)} valid dates, range: {date_range}")
                except Exception as e:
                    print(f"   ❌ Column {col}: Error validating dates - {e}")

    # 6. FINAL VALIDATION SUMMARY
    print("\n🔍 VALIDATION SUMMARY:")
    print(f"   ✅ Dataset shape: {df.shape}")
    print(f"   ✅ Required columns present: {len(required_columns)}/{len(required_columns)}")
    print(f"   ✅ ThroughputMbps calculated for all records")
    print(f"   ✅ Coordinates validated for all records")
    print(f"   ✅ Data ready for PostgreSQL storage")

    print("Mobile data validation completed successfully!")
    return True


def main():
    print("Iniciando proceso ETL...")

    # Initialize PostgreSQL handler
    postgres_handler = get_postgres_handler(
        host=get_env_var('POSTGRES_HOST'),
        port=get_env_var('POSTGRES_PORT'),
        database=get_env_var('POSTGRES_DB'),
        user=get_env_var('POSTGRES_USER'),
        password=get_env_var('POSTGRES_PASSWORD')
    )
    print(f"Conexión a PostgreSQL establecida: {get_env_var('POSTGRES_HOST')}:{get_env_var('POSTGRES_PORT')}")

    # Create tables if they don't exist
    postgres_handler.create_tables()
    print("Tablas verificadas/creadas en PostgreSQL")

    # Read the GeoJSON file
    gdf = gpd.read_file(f"{get_env_var('geojson_route')}/shapefile.shp")
    gdf = gdf.sort_values(['DPA_DESPRO', 'DPA_DESCAN', 'DPA_DESPAR'])
    print(f"Archivo shapefile cargado. Filas: {len(gdf)}")

    try:
        # Load mobile data from pre-extracted files
        print("Cargando datos pre-extraídos...")
        dataframes = load_mobile_data()

        if dataframes is None:
            print("Error: No se pudieron cargar los datos móviles")
            return

        # Process mobile data - EXACTLY like data_analysis_code.ipynb
        processed_mobile_data = process_mobile_data(dataframes)

        # Validate processed data
        validate_mobile_data(processed_mobile_data)

        # Store mobile data in PostgreSQL
        print("Storing mobile data in PostgreSQL...")
        postgres_handler.upsert_measurements(processed_mobile_data)
        print("Mobile data stored successfully!")

        # Handle geographic data
        print("Verificando estado de datos geográficos...")
        if postgres_handler.should_insert_geographic_data():
            print("Datos geográficos no encontrados en la base de datos. Procediendo con la inserción...")
            postgres_handler.upsert_geographic_data(gdf)
            print("Datos geográficos almacenados con éxito!")
        else:
            print("Datos geográficos ya existen en la base de datos. Omitiendo inserción.")

        print("Procesamiento de datos móviles y almacenamiento completado con éxito!")
        print(f"Procesados {len(processed_mobile_data)} registros móviles con ThroughputMbps")

    except Exception as e:
        print(f'Error al procesar los datos: {e}')
        # IMPORTANT: Re-raise the exception so Airflow knows the task failed
        raise

    print("Procesamiento de datos móviles completado con éxito!")


if __name__ == "__main__":
    main()
