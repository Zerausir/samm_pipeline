import os
import pandas as pd
import geopandas as gpd
import datetime
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
        # Leer los datos pre-extraídos en vez de conectarse a SQL Server
        print("Cargando datos pre-extraídos...")

        # Definir rutas de archivos
        data_dir = '/opt/airflow/app/data'  # Ruta en el contenedor
        df1_file = f"{data_dir}/extract_table1.parquet"
        df2_file = f"{data_dir}/extract_table2.parquet"

        # Verificar si los archivos existen
        if not os.path.exists(df1_file) or not os.path.exists(df2_file):
            print(f"ERROR: Archivos de datos no encontrados: {df1_file} o {df2_file}")
            print("Por favor, ejecuta el script de extracción extract_data.py en tu PC local primero.")
            return

        # Cargar datos desde archivos Parquet
        print(f"Leyendo archivo: {df1_file}")
        df1 = pd.read_parquet(df1_file)
        print(f"Leyendo archivo: {df2_file}")
        df2 = pd.read_parquet(df2_file)

        print(f"Datos cargados: {len(df1)} registros de tabla1, {len(df2)} registros de tabla2")

    except Exception as e:
        print(f'Error al cargar los datos pre-extraídos: {e}')
        return

    # Rename columns
    print("Procesando datos...")
    df1 = df1.rename(columns={'SessionIdOrCallIndex': 'SessionId', 'SessionEndStatus': 'EndServiceStatus'})
    df2 = df2.rename(columns={'StartDateTime': 'StartTime', 'EndDateTime': 'EndTime'})

    # Data type conversion
    df1['StartTime'] = pd.to_datetime(df1['StartTime'])
    df1['EndTime'] = pd.to_datetime(df1['EndTime'])
    df1['StartLatitude'] = df1['StartLatitude'].astype(float)
    df1['StartLongitude'] = df1['StartLongitude'].astype(float)
    df1['EndLatitude'] = df1['EndLatitude'].astype(float)
    df1['EndLongitude'] = df1['EndLongitude'].astype(float)
    df1['IMSI'] = df1['IMSI'].astype(str)
    df1['IMEI'] = df1['IMEI'].astype(str)
    df2['StartTime'] = pd.to_datetime(df2['StartTime'])
    df2['EndTime'] = pd.to_datetime(df2['EndTime'])
    df2['EndFileSize'] = df2['EndFileSize'].astype(float)

    # Merge dataframes
    print("Combinando datasets...")
    df = df1.merge(df2, how='right', on=['DatasourceId', 'SessionId', 'SessionType',
                                         'StartTime', 'EndTime', 'EndServiceStatus'])
    print(f"Resultado del merge: {len(df)} filas")

    # Read sense file data
    print(f"Cargando archivo sense desde: {get_env_var('sense_file')}")
    columns_sense = ['Device', 'IMEI', 'CZO']
    dfsense = pd.read_excel(get_env_var('sense_file'), usecols=columns_sense)
    dfsense['IMEI'] = dfsense['IMEI'].astype(str)
    print(f"Datos sense cargados: {len(dfsense)} filas")

    # Merge with sense data
    df = df.merge(dfsense, how='left', on=['IMEI'])
    print(f"Después del merge con sense data: {len(df)} filas")

    # Calculate throughput
    def calculate_throughput(row):
        if (row['EndFileSize'] != 0 and
                row['EndServiceStatus'] == 'Succeeded' and
                isinstance(row['DataTransferTimeMethodADuration'], datetime.time)):

            total_seconds = (
                    row['DataTransferTimeMethodADuration'].hour * 3600 +
                    row['DataTransferTimeMethodADuration'].minute * 60 +
                    row['DataTransferTimeMethodADuration'].second +
                    row['DataTransferTimeMethodADuration'].microsecond / 1000000
            )

            try:
                return (float(row['EndFileSize']) * 8) / total_seconds / 1000 / 1000
            except ZeroDivisionError:
                return None
        return None

    # Apply throughput calculation
    print("Calculando throughput...")
    df['ThroughputMbps'] = df.apply(calculate_throughput, axis=1)

    # Remove rows with NaN values in critical columns
    df = df.dropna(subset=['StartLatitude', 'StartLongitude', 'EndLatitude', 'EndLongitude', 'ThroughputMbps'])
    print(f"Después de filtrar nulos: {len(df)} filas")

    # Store data in PostgreSQL
    try:
        print("Almacenando datos de mediciones en PostgreSQL...")
        postgres_handler.upsert_measurements(df)
        print("Datos de mediciones almacenados con éxito!")

        print("Verificando estado de datos geográficos...")
        if postgres_handler.should_insert_geographic_data():
            print("Datos geográficos no encontrados en la base de datos. Procediendo con la inserción...")
            postgres_handler.upsert_geographic_data(gdf)
            print("Datos geográficos almacenados con éxito!")
        else:
            print("Datos geográficos ya existen en la base de datos. Omitiendo inserción.")
    except Exception as e:
        print(f"Error almacenando datos en PostgreSQL: {e}")
        return

    print("Procesamiento de datos y almacenamiento completado con éxito!")


if __name__ == "__main__":
    main()
