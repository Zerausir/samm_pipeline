import os
import pandas as pd
import datetime
from datetime import timedelta, datetime
import ipaddress
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
    """Process SessionSummary table (table1) exactly like CODIGOS_UNIDOS.ipynb"""
    print("Processing SessionSummary data...")

    # Convert datetime columns
    df['StartTime'] = pd.to_datetime(df['StartTime'], errors='coerce', format='%Y-%m-%d %H:%M:%S.%f')
    df['EndTime'] = pd.to_datetime(df['EndTime'], errors='coerce', format='%Y-%m-%d %H:%M:%S.%f')

    # Convert coordinate columns to float
    df['StartLatitude'] = df['StartLatitude'].astype(float)
    df['StartLongitude'] = df['StartLongitude'].astype(float)
    df['EndLatitude'] = df['EndLatitude'].astype(float)
    df['EndLongitude'] = df['EndLongitude'].astype(float)

    # Convert ID columns to Int64
    df['IMSI'] = pd.to_numeric(df['IMSI']).astype('Int64')
    df['IMEI'] = pd.to_numeric(df['IMEI']).astype('Int64')
    df['LogfileId'] = pd.to_numeric(df['LogfileId']).astype('Int64')

    # Process IP addresses
    def convertir_ip(ip):
        try:
            return ipaddress.ip_address(ip)
        except:
            return None

    df['IpAddress'] = df['IpAddress'].apply(convertir_ip)

    return df


def _process_table3_session_summary_voice(df):
    """Process SessionSummaryVoice table (table3) exactly like CODIGOS_UNIDOS.ipynb"""
    print("Processing SessionSummaryVoice data...")

    # Convert datetime columns
    datetime_columns = [
        'DialStartDateTime', 'CallAttemptCsfbDateTime', 'CallSetupDateTime',
        'CallSetupCsfbDateTime', 'CallEstablishedDateTime', 'DialEndDateTime',
        'CallInitiationDateTime', 'CallEndDateTime', 'EutranReselectionTimeAfterCsfbCallDateTime'
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
        'CallEstablishedUserCallEstablishedTime'
    ]
    for col in duration_columns:
        if col in df.columns:
            df[col] = df[col].apply(convertir_a_duracion)

    # Convert float columns
    df['AqmSessionEndAqmCallQuality'] = df['AqmSessionEndAqmCallQuality'].astype(float)
    df['AqmSessionEndAqmCallQualityDownlink'] = df['AqmSessionEndAqmCallQualityDownlink'].astype(float)

    # Convert ID columns to Int64
    id_columns = [
        'DatasourceId', 'CallIndex', 'IncompleteCall', 'DialStartSampleId',
        'DialStartPhoneNumber', 'CallAttemptRetrySampleId', 'CallAttemptSampleId',
        'Csfb', 'Srvcc', 'CallAttemptCsfbSampleId', 'CallSetupSampleId',
        'CallSetupCsfbSampleId', 'CallEstablishedSampleId', 'DialEndSampleId',
        'CallInitiationSampleId', 'CallEndSampleId', 'EutranReselectionTimeAfterCsfbCallSampleId',
        'AqmSessionEndOtherPartyPhoneNumber', 'AqmSessionEndPhoneNumber'
    ]
    for col in id_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    return df


def _process_table4_voice_quality(df):
    """Process SessionSummaryVoiceQuality table (table4) exactly like CODIGOS_UNIDOS.ipynb"""
    print("Processing SessionSummaryVoiceQuality data...")

    # Convert ID columns to Int64
    id_columns = ['DatasourceId', 'LastSampleId', 'CurrentCallIndex', 'RadioTechnology']
    for col in id_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    # Convert datetime
    if 'DateTime' in df.columns:
        df['DateTime'] = pd.to_datetime(df['DateTime'], errors='coerce', format='%Y-%m-%d %H:%M:%S.%f')

    # Convert coordinate columns
    df['Latitude'] = df['Latitude'].astype(float)
    df['Longitude'] = df['Longitude'].astype(float)

    # Convert specific AMR codec column mentioned in the notebook
    df['AmrCodecUsageDownlink_AMR12.2'] = df['AmrCodecUsageDownlink_AMR12.2'].astype(float)

    return df


def _process_sense_nacional(df):
    """Process sense_nacional Excel file exactly like df4 in CODIGOS_UNIDOS.ipynb"""
    print("Processing sense_nacional data...")

    # Convert IMSI and IMEI to Int64
    df['IMSI'] = pd.to_numeric(df['IMSI']).astype('Int64')
    df['IMEI'] = pd.to_numeric(df['IMEI']).astype('Int64')

    return df


def process_voice_data(dataframes):
    """
    Process and merge voice dataframes EXACTLY like CODIGOS_UNIDOS.ipynb
    """
    print("🚀 INICIANDO PROCESO DE MERGE COMPLETO")
    print("=" * 70)

    # Extract individual dataframes
    df1 = dataframes['table1'].copy()  # cdr_SessionSummary
    df2 = dataframes['table3'].copy()  # cdr_SessionSummaryVoice
    df3 = dataframes['table4'].copy()  # cdr_SessionSummaryVoiceQuality
    df4 = dataframes['sense_nacional'].copy()  # sense_nacional

    print(f"Table1 (SessionSummary) shape: {df1.shape}")
    print(f"Table3 (SessionSummaryVoice) shape: {df2.shape}")
    print(f"Table4 (SessionSummaryVoiceQuality) shape: {df3.shape}")
    print(f"sense_nacional shape: {df4.shape}")

    try:
        # 1. DATA PROCESSING - Apply transformations exactly like the notebook
        print("\n📱 PASO 1: PROCESANDO DATOS...")
        df1 = _process_table1_session_summary(df1)
        df2 = _process_table3_session_summary_voice(df2)
        df3 = _process_table4_voice_quality(df3)
        df4 = _process_sense_nacional(df4)

        # 2. MERGE PROCESS - Exactly like CODIGOS_UNIDOS.ipynb
        print("\n📱 PASO 2: INCORPORANDO DATOS DE DISPOSITIVOS (df1 + df4)")
        print("-" * 50)

        # Merge 1: df1 with df4 by (IMSI, IMEI)
        df1_4_merged = df1.merge(df4, on=['IMSI', 'IMEI'], how='left', suffixes=('', '_df4'))
        print(f"After merge df1+df4: {df1_4_merged.shape[0]} records")

        print("\n📞 PASO 3: INCORPORANDO DATOS DE VOZ (df1_4_merged + df2)")
        print("-" * 50)

        # Merge 2: Result with df2 by DatasourceId
        df1_4_2_merged = df1_4_merged.merge(df2, on='DatasourceId', how='left', suffixes=('', '_df2'))
        print(f"After merge +df2: {df1_4_2_merged.shape[0]} records")

        print("\n📡 PASO 4: INCORPORANDO DATOS DE RED (df1_4_2_merged + df3)")
        print("-" * 50)

        # Merge 3: Result with df3 by DatasourceId
        dataset_final = df1_4_2_merged.merge(df3, on='DatasourceId', how='left', suffixes=('', '_df3'))
        print(f"After merge +df3: {dataset_final.shape[0]} records")

        # Remove rows with NaN values in critical columns exactly like the notebook
        initial_rows = len(dataset_final)
        dataset_final = dataset_final.dropna(subset=['StartLatitude', 'StartLongitude', 'EndLatitude', 'EndLongitude'])
        final_rows = len(dataset_final)
        print(f"Filtrado de nulos: {initial_rows} → {final_rows} filas (-{initial_rows - final_rows})")

        print(f"\n🎯 RESUMEN FINAL DEL MERGE COMPLETO")
        print("=" * 70)
        print(f"📊 Dataset inicial (df1): {len(df1):,} filas × {len(df1.columns)} columnas")
        print(f"📊 Dataset final: {len(dataset_final):,} filas × {len(dataset_final.columns)} columnas")
        print(f"📈 Incremento de filas: +{len(dataset_final) - len(df1):,}")
        print(f"📈 Incremento de columnas: +{len(dataset_final.columns) - len(df1.columns)}")

        # Data quality summary
        dispositivos_enriquecidos = dataset_final['Device'].notna().sum()
        columnas_df2 = [col for col in df2.columns if col != 'DatasourceId']
        columnas_df3 = [col for col in df3.columns if col != 'DatasourceId']
        voz_enriquecida = dataset_final[columnas_df2].notna().any(axis=1).sum()
        red_enriquecida = dataset_final[columnas_df3].notna().any(axis=1).sum()

        print(f"\n📋 ENRIQUECIMIENTO DE DATOS:")
        print(
            f"   📱 Registros con datos de dispositivo: {dispositivos_enriquecidos:,} ({(dispositivos_enriquecidos / len(dataset_final) * 100):.1f}%)")
        print(
            f"   📞 Registros con datos de voz: {voz_enriquecida:,} ({(voz_enriquecida / len(dataset_final) * 100):.1f}%)")
        print(
            f"   📡 Registros con datos de red: {red_enriquecida:,} ({(red_enriquecida / len(dataset_final) * 100):.1f}%)")

        print(f"\n🚀 EL MERGE SE COMPLETÓ EXITOSAMENTE")
        print("=" * 70)

        return dataset_final

    except Exception as e:
        print(f"Error in process_voice_data: {e}")
        raise


def validate_voice_data(df):
    """
    Validate processed voice data before storage
    Enhanced validation to match the final dataset structure
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

    # Check IMEI if present
    if 'IMEI' in df.columns:
        non_null_imei = df['IMEI'].notna().sum()
        print(f"Valid IMEI records: {non_null_imei}/{len(df)} ({(non_null_imei / len(df) * 100):.1f}%)")

    # 3. DATETIME VALIDATION
    print("Validating datetime fields...")
    datetime_columns = [col for col in df.columns if
                        'DateTime' in col or 'Time' in col and df[col].dtype == 'datetime64[ns]']

    for col in datetime_columns:
        if col in df.columns:
            valid_dates = df[col].notna().sum()
            if valid_dates > 0:
                try:
                    datetime_series = pd.to_datetime(df[col], errors='coerce')
                    valid_datetime_series = datetime_series.dropna()
                    if len(valid_datetime_series) > 0:
                        date_range = f"{valid_datetime_series.min()} to {valid_datetime_series.max()}"
                        print(f"Column {col}: {len(valid_datetime_series)} valid dates, range: {date_range}")
                except Exception as e:
                    print(f"Column {col}: Error validating dates - {e}")

    # 4. COORDINATE VALIDATION
    print("Validating coordinates...")
    coordinate_pairs = [
        ('StartLatitude', 'StartLongitude'),
        ('EndLatitude', 'EndLongitude'),
        ('Latitude', 'Longitude')
    ]

    for lat_col, lon_col in coordinate_pairs:
        if lat_col in df.columns and lon_col in df.columns:
            try:
                lat_numeric = pd.to_numeric(df[lat_col], errors='coerce')
                lon_numeric = pd.to_numeric(df[lon_col], errors='coerce')

                valid_coords = ((lat_numeric.between(-90, 90)) &
                                (lon_numeric.between(-180, 180))).sum()
                total_coords = df[[lat_col, lon_col]].notna().all(axis=1).sum()
                if total_coords > 0:
                    print(f"Valid coordinate pairs ({lat_col}, {lon_col}): {valid_coords}/{total_coords}")
            except Exception as e:
                print(f"Error validating coordinates ({lat_col}, {lon_col}): {e}")

    # 5. VOICE CALL SPECIFIC VALIDATION
    print("Validating voice call specifics...")

    # Check call directions if present
    if 'CallDirection' in df.columns:
        try:
            call_directions = df['CallDirection'].value_counts()
            print(f"Call directions: {dict(call_directions)}")
        except Exception as e:
            print(f"Error analyzing call directions: {e}")

    # 6. DATA COMPLETENESS ANALYSIS
    print("Analyzing data completeness...")
    try:
        # Calculate completeness per column
        completeness = ((df.notna().sum() / len(df)) * 100).round(1)

        # Report columns with high completeness
        high_completeness = completeness[completeness >= 90]
        print(f"Columns with ≥90% completeness: {len(high_completeness)}")

        # Report overall completeness
        total_complete_records = df.notna().any(axis=1).sum()
        print(f"Records with any data: {total_complete_records}")
        print(f"Overall data completeness: {(df.notna().sum().sum() / (len(df) * len(df.columns)) * 100):.1f}%")

    except Exception as e:
        print(f"Error analyzing data completeness: {e}")

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

        # Process voice data - EXACTLY like CODIGOS_UNIDOS.ipynb
        processed_voice_data = process_voice_data(dataframes)

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
