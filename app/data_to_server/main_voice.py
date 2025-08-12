import os
import pandas as pd
import datetime
from datetime import timedelta, datetime
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


def load_voice_data():
    """
    Load voice data from parquet files and Excel file and return as separate dataframes
    """
    # Define data directory
    data_dir = '/opt/airflow/app/data'  # Path in container

    # Define file paths
    voice_files = {
        'table1': f"{data_dir}/extract_voz_table1.parquet",
        'table3': f"{data_dir}/extract_voz_table3.parquet",
        'table4': f"{data_dir}/extract_voz_table4.parquet",
        'sense_nacional': f"{data_dir}/sense_nacional_v0.xlsx"  # Changed from table5 parquet to Excel
    }

    # Verify files exist
    missing_files = []
    for table_name, file_path in voice_files.items():
        if not os.path.exists(file_path):
            missing_files.append(file_path)

    if missing_files:
        print(f"ERROR: Voice data files not found: {', '.join(missing_files)}")
        print("Please run the data extraction script on your local PC first.")
        return None

    # Load data from files
    dataframes = {}

    # Load parquet files
    for table_name in ['table1', 'table3', 'table4']:
        try:
            print(f"Reading file: {voice_files[table_name]}")
            df = pd.read_parquet(voice_files[table_name])
            dataframes[table_name] = df
            print(f"Loaded {table_name}: {len(df)} records")
        except Exception as e:
            print(f"Error loading {voice_files[table_name]}: {e}")
            return None

    # Load Excel file for sense_nacional
    try:
        print(f"Reading file: {voice_files['sense_nacional']}")
        df = pd.read_excel(voice_files['sense_nacional'])
        dataframes['sense_nacional'] = df
        print(f"Loaded sense_nacional: {len(df)} records")
    except Exception as e:
        print(f"Error loading {voice_files['sense_nacional']}: {e}")
        return None

    return dataframes


def _process_table1_session_summary(df):
    """Process SessionSummary table (table1) with only required columns"""
    print("Processing SessionSummary data...")

    # FILTER FIRST: Only keep Voice MO sessions to reduce data volume
    initial_rows = len(df)
    if 'SessionType' in df.columns:
        df = df[df['SessionType'] == 'Voice MO'].copy()
        filtered_rows = len(df)
        print(
            f"Filtered for Voice MO sessions: {initial_rows:,} → {filtered_rows:,} records (-{initial_rows - filtered_rows:,})")
    else:
        print("Warning: SessionType column not found, processing all records")

    if df.empty:
        print("Warning: No Voice MO sessions found after filtering!")
        return df

    # Define required columns
    required_columns = [
        'DatasourceId', 'SessionIdOrCallIndex', 'SessionType', 'StartTime', 'StartLatitude', 'StartLongitude',
        'StartRadioTechnology', 'EndSessionNetworkIndex', 'EndTime', 'EndLatitude',
        'EndLongitude', 'EndRadioTechnology', 'Operator', 'SimOperator', 'IMSI', 'IMEI',
        'SessionEndStatus', 'ErrorCause', 'ErrorCauseDetails', 'RadioTechnologySequence', 'NoGps'
    ]

    # Filter to only required columns (keep only those that exist)
    existing_columns = [col for col in required_columns if col in df.columns]
    initial_columns = len(df.columns)
    df = df[existing_columns].copy()
    filtered_columns = len(df.columns)

    print(f"Filtered columns for voice table1: {initial_columns} → {filtered_columns} columns")
    print(f"   - Columns kept: {existing_columns}")

    missing_columns = [col for col in required_columns if col not in existing_columns]
    if missing_columns:
        print(f"   - Missing columns: {missing_columns}")

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
            f"Removed duplicates from voice table1: {before_dedup:,} → {after_dedup:,} records (-{duplicates_removed:,} duplicates)")

        if duplicates_removed > 0:
            print(f"   - Duplicate detection based on columns: {existing_duplicate_columns}")
    else:
        print("Warning: No duplicate detection columns found in voice table1")

    # Convert datetime columns
    datetime_columns = ['StartTime', 'EndTime']
    for col in datetime_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', format='%Y-%m-%d %H:%M:%S.%f')

    # Convert coordinate columns to float
    coordinate_columns = ['StartLatitude', 'StartLongitude', 'EndLatitude', 'EndLongitude']
    for col in coordinate_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)

    # Convert ID and numeric columns
    id_columns = ['DatasourceId', 'IMSI', 'IMEI', 'EndSessionNetworkIndex']
    for col in id_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    # Convert boolean columns
    boolean_columns = ['NoGps']
    for col in boolean_columns:
        if col in df.columns:
            df[col] = df[col].astype('boolean')

    return df


def _process_table3_session_summary_voice(df):
    """Process SessionSummaryVoice table (table3) with only required columns"""
    print("Processing SessionSummaryVoice data...")

    # Define required columns
    required_columns = [
        'DatasourceId', 'CallIndex', 'CallDirection', 'AqmCallType', 'StartRadioTechnology', 'EndRadioTechnology',
        'DialStartDateTime', 'DialStartPhoneNumber', 'CallAttemptDateTime', 'CallAttemptRadioTechnology',
        'CallAttemptCsfbDateTime', 'CallAttemptCsfRadioTechnology', 'CallEstablishedDateTime',
        'CallEstablishedRadioTechnology', 'CallEstablishedDomain', 'DialEndDateTime', 'DialEndServiceStatus',
        'CallInitiationDateTime', 'CallInitiationRadioTechnology', 'CallReestablishedDateTime', 'CallEndDateTime',
        'CallEndRadioTechnology', 'CallEndCause', 'CallEndType', 'CallEndDomain', 'CallEndCallDuration',
        'AqmSessionEndOtherPartyPhoneNumber', 'AqmSessionEndPhoneNumber', 'AqmSessionEndAqmCallQuality',
        'AqmSessionEndAqmCallQualityDownlink', 'AqmSessionEndAqmCallQualityUplink', 'AqmAlgorithmDownlink',
        'AqmAlgorithmUplink', 'SpeechCodecs', 'SpeechPathDelayOneWay', 'SilentCall',
        'SpeechInterruptionTimeDownlinkDuration', 'RtpInterruptionTimeAudioInterruptionTime',
        'HandoverSpeechInterruptionDownlinkDuration', 'CallSetupOffHookTime', 'CallAttemptAccessNetworkInfo',
        'CallSetupAccessNetworkInfo', 'CallSetupDomain', 'CallSetupUserPerceivedCallSetupTime',
        'CallEstablishedUserCallEstablishedTime', 'CallEstablishedCallAnswerDelay', 'BearerTechnology'
    ]

    # Filter to only required columns (keep only those that exist)
    existing_columns = [col for col in required_columns if col in df.columns]
    initial_columns = len(df.columns)
    df = df[existing_columns].copy()
    filtered_columns = len(df.columns)

    print(f"Filtered columns for table3: {initial_columns} → {filtered_columns} columns")
    print(f"   - Columns kept: {existing_columns}")

    missing_columns = [col for col in required_columns if col not in existing_columns]
    if missing_columns:
        print(f"   - Missing columns: {missing_columns}")

    # REMOVE DUPLICATES BEFORE DATA TYPE CONVERSION
    duplicate_columns = [
        'DatasourceId', 'StartRadioTechnology', 'EndRadioTechnology', 'DialStartDateTime',
        'CallAttemptDateTime', 'CallAttemptRadioTechnology', 'CallEndDateTime',
        'CallEndRadioTechnology', 'CallEndCause'
    ]

    # Only use columns that exist in the dataframe
    existing_duplicate_columns = [col for col in duplicate_columns if col in df.columns]

    if existing_duplicate_columns:
        before_dedup = len(df)
        df = df.drop_duplicates(subset=existing_duplicate_columns, keep='first')
        after_dedup = len(df)
        duplicates_removed = before_dedup - after_dedup
        print(
            f"Removed duplicates from voice table3: {before_dedup:,} → {after_dedup:,} records (-{duplicates_removed:,} duplicates)")

        if duplicates_removed > 0:
            print(f"   - Duplicate detection based on columns: {existing_duplicate_columns}")
    else:
        print("Warning: No duplicate detection columns found in voice table3")

    # Convert datetime columns
    datetime_columns = [
        'DialStartDateTime', 'CallAttemptCsfbDateTime', 'CallSetupDateTime',
        'CallSetupCsfbDateTime', 'CallEstablishedDateTime', 'DialEndDateTime',
        'CallInitiationDateTime', 'CallEndDateTime', 'EutranReselectionTimeAfterCsfbCallDateTime',
        'CallAttemptDateTime', 'CallBlockedDateTime', 'CallReestablishedDateTime'
    ]
    for col in datetime_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', format='%Y-%m-%d %H:%M:%S.%f')

    # Function to convert to timedelta
    def convertir_a_duracion(valor):
        try:
            valor = str(valor).strip()[:15]  # Limit to 15 chars
            t = datetime.strptime(valor, "%H:%M:%S.%f")
            return timedelta(hours=t.hour, minutes=t.minute, seconds=t.second, microseconds=t.microsecond)
        except:
            return None

    # Apply conversion to duration columns
    duration_columns = [
        'CallSetupTime', 'CallSetupUserPerceivedTime', 'CallSetupServiceRequestTime',
        'CallSetupCsfbTime', 'CallSetupCsfbUserPerceivedTime', 'CallSetupCsfbServiceRequestTime',
        'CallEndCallDuration', 'EutranReselectionTimeAfterCsfbCallIdleToLteTime',
        'CallSetupOffHookTime', 'CallSetupUserPerceivedCallSetupTime',
        'CallEstablishedUserCallEstablishedTime', 'CallBlockedDuration', 'CallBlockedCsfbDuration'
    ]
    for col in duration_columns:
        if col in df.columns:
            df[col] = df[col].apply(convertir_a_duracion)

    # Convert float columns
    float_columns = [
        'AqmSessionEndAqmCallQuality', 'AqmSessionEndAqmCallQualityDownlink'
    ]
    for col in float_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)

    # Convert ID columns to Int64
    id_columns = [
        'DatasourceId', 'DialStartPhoneNumber', 'DialEndSampleId',
        'AqmSessionEndOtherPartyPhoneNumber', 'AqmSessionEndPhoneNumber'
    ]
    for col in id_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    return df


def _process_table4_voice_quality(df):
    """Process SessionVoiceQuality table (table4) with only required columns and coordinate filtering"""
    print("Processing SessionVoiceQuality data...")

    # Define required columns
    required_columns = [
        'DatasourceId', 'CallIndex', 'SentenceIndex', 'StartDateTime', 'EndDateTime', 'EndLatitude', 'EndLongitude',
        'AqmScoreAny', 'AqmScoreDownlink', 'AqmScoreUplink', 'SpeechCodec',
        'AqmAlgorithmDownlink', 'AqmAlgorithmUplink'
    ]

    # Filter to only required columns (keep only those that exist)
    existing_columns = [col for col in required_columns if col in df.columns]
    initial_columns = len(df.columns)
    df = df[existing_columns].copy()
    filtered_columns = len(df.columns)

    print(f"Filtered columns for table4: {initial_columns} → {filtered_columns} columns")
    print(f"   - Columns kept: {existing_columns}")

    missing_columns = [col for col in required_columns if col not in existing_columns]
    if missing_columns:
        print(f"   - Missing columns: {missing_columns}")

    # FILTER OUT NULL COORDINATES BEFORE DUPLICATE REMOVAL
    if 'EndLatitude' in df.columns and 'EndLongitude' in df.columns:
        initial_rows = len(df)
        df = df.dropna(subset=['EndLatitude', 'EndLongitude'])
        filtered_rows = len(df)
        print(
            f"Filtered null coordinates: {initial_rows:,} → {filtered_rows:,} records (-{initial_rows - filtered_rows:,})")
    else:
        print("Warning: EndLatitude or EndLongitude columns not found - skipping coordinate filtering")

    if df.empty:
        print("Warning: No records remaining after coordinate filtering!")
        return df

    # REMOVE DUPLICATES BEFORE DATA TYPE CONVERSION
    duplicate_columns = [
        'DatasourceId', 'StartDateTime', 'EndDateTime', 'EndLatitude',
        'EndLongitude', 'EndRadioTechnology'
    ]

    # Only use columns that exist in the dataframe
    existing_duplicate_columns = [col for col in duplicate_columns if col in df.columns]

    if existing_duplicate_columns:
        before_dedup = len(df)
        df = df.drop_duplicates(subset=existing_duplicate_columns, keep='first')
        after_dedup = len(df)
        duplicates_removed = before_dedup - after_dedup
        print(
            f"Removed duplicates from voice table4: {before_dedup:,} → {after_dedup:,} records (-{duplicates_removed:,} duplicates)")

        if duplicates_removed > 0:
            print(f"   - Duplicate detection based on columns: {existing_duplicate_columns}")
    else:
        print("Warning: No duplicate detection columns found in voice table4")

    # Convert ID columns to Int64
    id_columns = ['DatasourceId']
    for col in id_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    # Convert datetime
    datetime_columns = ['StartDateTime', 'EndDateTime']
    for col in datetime_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', format='%Y-%m-%d %H:%M:%S.%f')

    # Convert coordinate columns to float
    coordinate_columns = ['EndLatitude', 'EndLongitude']
    for col in coordinate_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)

    # Convert quality score columns to float
    score_columns = ['AqmScoreAny', 'AqmScoreDownlink', 'AqmScoreUplink']
    for col in score_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)

    return df


def _process_sense_nacional(df):
    """Process sense_nacional Excel file exactly like df4 in CODIGOS_UNIDOS.ipynb"""
    print("Processing sense_nacional data...")

    # Convert IMSI and IMEI to Int64
    df['IMSI'] = pd.to_numeric(df['IMSI']).astype('Int64')
    df['IMEI'] = pd.to_numeric(df['IMEI']).astype('Int64')

    return df


def process_voice_data_chunked(dataframes, chunk_size=10000):
    """
    Process and merge voice dataframes starting with the largest table (df3)
    Using chunks for the largest merge (df3 + df2)
    """
    print("🚀 INICIANDO PROCESO DE MERGE COMPLETO (CHUNKED) - ORDEN CORRECTO")
    print("=" * 70)

    # Extract individual dataframes
    df1 = dataframes['table1'].copy()  # cdr_SessionSummary
    df2 = dataframes['table3'].copy()  # cdr_SessionSummaryVoice
    df3 = dataframes['table4'].copy()  # cdr_SessionVoiceQuality (LA MÁS GRANDE)
    df4 = dataframes['sense_nacional'].copy()  # sense_nacional

    # OPTIMIZACIÓN 1: LIBERAR MEMORIA INMEDIATAMENTE
    del dataframes  # Liberar dataframes originales
    gc.collect()
    print("✅ Memoria de dataframes originales liberada")

    print(f"Table1 (SessionSummary) shape: {df1.shape}")
    print(f"Table3 (SessionSummaryVoice) shape: {df2.shape}")
    print(f"Table4 (SessionVoiceQuality) shape: {df3.shape} (LA MÁS GRANDE)")
    print(f"sense_nacional shape: {df4.shape}")
    print(f"Chunk size: {chunk_size:,}")

    try:
        # 1. DATA PROCESSING - Apply transformations
        print("\n📱 PASO 1: PROCESANDO DATOS...")
        df1 = _process_table1_session_summary(df1)
        df2 = _process_table3_session_summary_voice(df2)
        df3 = _process_table4_voice_quality(df3)
        df4 = _process_sense_nacional(df4)

        # OPTIMIZACIÓN 2: VALIDAR QUE TENEMOS DATOS PARA HACER MERGE
        if df1.empty:
            raise ValueError("❌ CRITICAL: Table1 (SessionSummary) is empty after processing")
        if df2.empty:
            raise ValueError("❌ CRITICAL: Table3 (SessionSummaryVoice) is empty after processing")
        if df3.empty:
            raise ValueError("❌ CRITICAL: Table4 (SessionVoiceQuality) is empty after processing")
        if df4.empty:
            raise ValueError("❌ CRITICAL: sense_nacional is empty after processing")

        print("✅ Todas las tablas contienen datos después del procesamiento")

        # 2. MERGE 1 EN CHUNKS: df3 (VoiceQuality) + df2 (SessionSummaryVoice)
        print("\n📡 PASO 2: MERGE df3 + df2 EN CHUNKS (VoiceQuality + SessionSummaryVoice)")
        print("-" * 50)
        print(f"Merging on: DatasourceId, CallIndex")

        processed_chunks = []
        num_chunks = len(df3) // chunk_size + (1 if len(df3) % chunk_size > 0 else 0)
        failed_chunks = 0

        print(f"Processing {len(df3):,} records in {num_chunks} chunks of {chunk_size:,}")

        for i in range(0, len(df3), chunk_size):
            chunk_end = min(i + chunk_size, len(df3))
            chunk_num = i // chunk_size + 1

            print(f"Processing chunk {chunk_num}/{num_chunks}: rows {i:,} to {chunk_end:,}")

            # OPTIMIZACIÓN 3: MANEJO DE ERRORES EN CHUNKS
            try:
                # Get chunk from df3
                chunk_df3 = df3.iloc[i:chunk_end].copy()

                # Merge chunk with df2
                chunk_merged = chunk_df3.merge(df2, on=['DatasourceId', 'CallIndex'], how='left', suffixes=('', '_df2'))

                print(
                    f"  Chunk {chunk_num}: df3 chunk ({len(chunk_df3)}) + df2 ({len(df2)}) = {len(chunk_merged)} records")

                if len(chunk_merged) > 0:
                    processed_chunks.append(chunk_merged)
                else:
                    print(f"⚠️  WARNING: Chunk {chunk_num} resulted in 0 records after merge")

                # Clear chunk memory
                del chunk_df3, chunk_merged
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
            df3_2_merged = pd.concat(processed_chunks, ignore_index=True)
            print(f"Combined dataset after MERGE 1: {len(df3_2_merged):,} records")

            # OPTIMIZACIÓN 2: VALIDAR MERGE 1
            if len(df3_2_merged) == 0:
                raise ValueError("❌ MERGE 1 FAILED: No matching records between VoiceQuality and SessionSummaryVoice")

            print("✅ MERGE 1 exitoso")
        else:
            raise ValueError("❌ MERGE 1 FAILED: No chunks were processed successfully!")

        # Clear memory
        del processed_chunks, df2, df3
        gc.collect()

        # 3. MERGE 2 NORMAL: Resultado + df1 (SessionSummary)
        print("\n📞 PASO 3: MERGE RESULTADO + df1 (+ SessionSummary) - NORMAL")
        print("-" * 50)
        print(f"Merging on: DatasourceId and CallIndex=SessionIdOrCallIndex")

        # Rename column for the merge to work properly
        if 'SessionIdOrCallIndex' in df1.columns:
            df1_renamed = df1.rename(columns={'SessionIdOrCallIndex': 'CallIndex'})
            merge_columns = ['DatasourceId', 'CallIndex']
        else:
            print("Warning: SessionIdOrCallIndex not found in df1, using only DatasourceId")
            df1_renamed = df1
            merge_columns = ['DatasourceId']

        df3_2_1_merged = df3_2_merged.merge(df1_renamed, on=merge_columns, how='left', suffixes=('', '_df1'))
        print(f"After MERGE 2: {len(df3_2_1_merged):,} records")

        # OPTIMIZACIÓN 2: VALIDAR MERGE 2
        if len(df3_2_1_merged) == 0:
            raise ValueError("❌ MERGE 2 FAILED: No matching records with SessionSummary")

        print("✅ MERGE 2 exitoso")

        # Clear memory
        del df3_2_merged, df1, df1_renamed
        gc.collect()

        # 4. MERGE 3 NORMAL: Resultado + df4 (sense_nacional)
        print("\n📱 PASO 4: MERGE RESULTADO + df4 (+ sense_nacional) - NORMAL")
        print("-" * 50)
        print(f"Merging on: IMSI, IMEI")

        dataset_final = df3_2_1_merged.merge(df4, on=['IMSI', 'IMEI'], how='left', suffixes=('', '_df4'))
        print(f"After MERGE 3 (final): {len(dataset_final):,} records")

        # OPTIMIZACIÓN 2: VALIDAR MERGE 3 (FINAL)
        if len(dataset_final) == 0:
            raise ValueError("❌ MERGE 3 FAILED: Final dataset is empty")

        print("✅ MERGE 3 exitoso")

        # Remove rows with NaN values in critical coordinate columns
        print("\n🧹 LIMPIEZA FINAL DE COORDENADAS...")
        initial_rows = len(dataset_final)

        # Check which coordinate columns exist and filter accordingly
        coordinate_columns_to_check = []
        if 'StartLatitude' in dataset_final.columns and 'StartLongitude' in dataset_final.columns:
            coordinate_columns_to_check.extend(['StartLatitude', 'StartLongitude'])
        if 'EndLatitude' in dataset_final.columns and 'EndLongitude' in dataset_final.columns:
            coordinate_columns_to_check.extend(['EndLatitude', 'EndLongitude'])

        if coordinate_columns_to_check:
            dataset_final = dataset_final.dropna(subset=coordinate_columns_to_check)
            final_rows = len(dataset_final)
            print(f"Filtered coordinates: {initial_rows:,} → {final_rows:,} records (-{initial_rows - final_rows:,})")

            # VALIDAR QUE AÚN TENEMOS DATOS DESPUÉS DE FILTRAR COORDENADAS
            if final_rows == 0:
                raise ValueError("❌ CRITICAL: No records remaining after coordinate filtering!")
        else:
            final_rows = initial_rows
            print(f"No coordinate filtering needed: {initial_rows:,} records")

        # Clear final memory
        del df3_2_1_merged, df4
        gc.collect()

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

        # Check session summary enrichment
        session_cols = [col for col in dataset_final.columns if col.endswith('_df1')]
        if session_cols:
            session_enriched = dataset_final[session_cols].notna().any(axis=1).sum()
            print(
                f"   📞 Registros enriquecidos con datos de sesión: {session_enriched:,} ({(session_enriched / len(dataset_final) * 100):.1f}%)")

        # Check voice summary enrichment
        voice_cols = [col for col in dataset_final.columns if col.endswith('_df2')]
        if voice_cols:
            voice_enriched = dataset_final[voice_cols].notna().any(axis=1).sum()
            print(
                f"   🎙️  Registros enriquecidos con datos de voz: {voice_enriched:,} ({(voice_enriched / len(dataset_final) * 100):.1f}%)")

        print(f"\n🚀 EL MERGE SE COMPLETÓ EXITOSAMENTE")
        print("📈 ESTRATEGIA DE CHUNKS APLICADA AL MERGE MÁS GRANDE (df3+df2)")
        print("✅ OPTIMIZACIONES IMPLEMENTADAS:")
        print("   - Gestión de memoria mejorada")
        print("   - Validación de integridad en cada merge")
        print("   - Manejo de errores robusto en chunks")
        print("=" * 70)

        return dataset_final

    except Exception as e:
        print(f"❌ Error in process_voice_data_chunked: {e}")
        # Asegurar limpieza de memoria antes de abortar
        gc.collect()
        raise


def validate_voice_data(df):
    """
    Validate processed voice data before storage (optimized for large datasets)
    """
    print("Validating voice data...")

    # Basic validation checks
    if df.empty:
        raise ValueError("Processed dataframe is empty")

    print(f"Validating {len(df)} voice records with {len(df.columns)} columns")

    # 1. REQUIRED COLUMNS CHECK
    required_columns = ['DatasourceId']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # 2. KEY FIELD VALIDATION
    print("Validating key fields...")

    # Check DatasourceId (should not be all null)
    if df['DatasourceId'].isnull().all():
        raise ValueError("All DatasourceId values are null")

    # 3. COORDINATE VALIDATION (simplified)
    print("Validating coordinates...")
    coordinate_pairs = [
        ('StartLatitude', 'StartLongitude'),
        ('EndLatitude', 'EndLongitude'),
        ('EndLatitude_df3', 'EndLongitude_df3')
    ]

    for lat_col, lon_col in coordinate_pairs:
        if lat_col in df.columns and lon_col in df.columns:
            try:
                valid_coords = ((df[lat_col].between(-90, 90)) &
                                (df[lon_col].between(-180, 180))).sum()
                total_coords = df[[lat_col, lon_col]].notna().all(axis=1).sum()
                if total_coords > 0:
                    print(f"Valid coordinate pairs ({lat_col}, {lon_col}): {valid_coords}/{total_coords}")
            except Exception as e:
                print(f"Error validating coordinates ({lat_col}, {lon_col}): {e}")

    print("Voice data validation completed successfully!")
    return True


def main():
    print("Iniciando proceso ETL de datos de voz...")

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

    try:
        # Load voice data from pre-extracted files
        print("Cargando datos de voz pre-extraídos...")
        dataframes = load_voice_data()

        if dataframes is None:
            print("Error: No se pudieron cargar los datos de voz")
            return

        # Process voice data in chunks to manage memory
        processed_voice_data = process_voice_data_chunked(dataframes, chunk_size=8000)

        # Validate processed data
        validate_voice_data(processed_voice_data)

        # Store voice data in PostgreSQL
        print("Storing voice data in PostgreSQL...")
        postgres_handler.upsert_voice_measurements(processed_voice_data)
        print("Voice data stored successfully!")

        print("Voice data processing completed successfully!")
        print(f"Processed {len(processed_voice_data)} voice records")

    except Exception as e:
        print(f'Error processing voice data: {e}')
        # IMPORTANT: Re-raise the exception so Airflow knows the task failed
        raise

    print("Procesamiento de datos de voz completado con éxito!")


if __name__ == "__main__":
    main()
