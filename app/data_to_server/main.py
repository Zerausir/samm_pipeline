import os
import pandas as pd
import geopandas as gpd
import sqlalchemy as sa
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
from dotenv import load_dotenv
import datetime
from app.utils.postgres_handler import get_postgres_handler


def main():
    # Load environment variables
    load_dotenv()

    # Initialize PostgreSQL handler
    postgres_handler = get_postgres_handler(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )

    # Create tables if they don't exist
    postgres_handler.create_tables()

    # Read the GeoJSON file
    gdf = gpd.read_file(f"{os.getenv('geojson_route')}/shapefile.shp")
    gdf = gdf.sort_values(['DPA_DESPRO', 'DPA_DESCAN', 'DPA_DESPAR'])

    # Define the date range
    fecha_inicio = datetime.datetime(2025, 1, 1)
    fecha_fin = datetime.datetime.now()

    # Format dates for SQL Server
    fecha_inicio_str = fecha_inicio.strftime('%Y-%m-%d %H:%M:%S')
    fecha_fin_str = fecha_fin.strftime('%Y-%m-%d %H:%M:%S')

    # SQL Server connection and queries
    sql_query1 = f"SELECT DatasourceId, SessionIdOrCallIndex, SessionType, StartTime, StartLatitude, " \
                 f"StartLongitude, StartRadioTechnology, EndTime, EndLatitude, EndLongitude, " \
                 f"EndRadioTechnology, SimOperator, IMSI, IMEI, SessionEndStatus " \
                 f"FROM {os.getenv('TABLE1')} " \
                 f"WHERE StartTime >= '{fecha_inicio_str}' AND EndTime <= '{fecha_fin_str}';"
    sql_query2 = f"SELECT DatasourceId, SessionId, SessionType, StartDateTime, EndDateTime, Url, " \
                 f"EndServiceBearer, EndDataRadioBearer, EndFileSize, EndServiceStatus, " \
                 f"IPServiceSetupTimeMethodAMethod, DataTransferTimeMethodADuration " \
                 f"FROM {os.getenv('TABLE2')} " \
                 f"WHERE StartDateTime >= '{fecha_inicio_str}' AND EndDateTime <= '{fecha_fin_str}';"

    try:
        # Create the engine according to SQLAlchemy documentation
        connection_string = f"DRIVER={os.getenv('DRIVER_NAME')};SERVER={os.getenv('SERVER_NAME')};" \
                            f"DATABASE={os.getenv('DATABASE_NAME')};Trusted_Connection=yes;"
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        engine = create_engine(connection_url)

        # Define decimal separator as comma
        decimal_sep = ","

        # Execute queries using SQLAlchemy
        with engine.connect() as connection:
            df1 = pd.read_sql_query(sa.text(sql_query1), connection, params={"decimal": decimal_sep})
            df2 = pd.read_sql_query(sa.text(sql_query2), connection, params={"decimal": decimal_sep})

    except Exception as e:
        print(f'SQL Server connection failed: {e}')
        return

    # Rename columns
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
    df = df1.merge(df2, how='right', on=['DatasourceId', 'SessionId', 'SessionType',
                                         'StartTime', 'EndTime', 'EndServiceStatus'])

    # Read sense file data
    columns_sense = ['Device', 'IMEI', 'CZO']
    dfsense = pd.read_excel(os.getenv('sense_file'), usecols=columns_sense)
    dfsense['IMEI'] = dfsense['IMEI'].astype(str)

    # Merge with sense data
    df = df.merge(dfsense, how='left', on=['IMEI'])

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
    df['ThroughputMbps'] = df.apply(calculate_throughput, axis=1)

    # Remove rows with NaN values in critical columns
    df = df.dropna(subset=['StartLatitude', 'StartLongitude', 'EndLatitude', 'EndLongitude', 'ThroughputMbps'])
    print(f"Processing {len(df)} records...")

    # Store data in PostgreSQL
    try:
        print("Storing measurement data in PostgreSQL...")
        postgres_handler.upsert_measurements(df)
        print("Measurement data stored successfully!")

        print("Checking geographic data status...")
        if postgres_handler.should_insert_geographic_data():
            print("Geographic data not found in database. Proceeding with insertion...")
            postgres_handler.upsert_geographic_data(gdf)
            print("Geographic data stored successfully!")
        else:
            print("Geographic data already exists in database. Skipping insertion.")
    except Exception as e:
        print(f"Error storing data in PostgreSQL: {e}")
        return

    print("Data processing and storage completed successfully!")


if __name__ == "__main__":
    main()
