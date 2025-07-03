import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import datetime
import hashlib
import json
from typing import Dict, List, Any
import geopandas as gpd


class PostgresDataHandler:
    def __init__(self, connection_params: Dict[str, str]):
        """Initialize PostgreSQL connection parameters"""
        self.connection_params = connection_params

    def _get_connection(self):
        """Create and return a new database connection"""
        return psycopg2.connect(
            dbname=self.connection_params['database'],
            user=self.connection_params['user'],
            password=self.connection_params['password'],
            host=self.connection_params['host'],
            port=self.connection_params['port']
        )

    def create_tables(self):
        """Create tables if they don't exist"""
        mobile_measurements_table = """
            CREATE TABLE IF NOT EXISTS mobile_measurements (
                measurement_id VARCHAR PRIMARY KEY,
                valid_from TIMESTAMP NOT NULL,
                valid_to TIMESTAMP,
                is_current INTEGER DEFAULT 1,
                batch_id VARCHAR,
                ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """

        geographic_table = """
            CREATE TABLE IF NOT EXISTS geographic_regions (
                region_id VARCHAR PRIMARY KEY,
                valid_from TIMESTAMP NOT NULL,
                valid_to TIMESTAMP,
                is_current INTEGER DEFAULT 1,
                dpa_despar VARCHAR,
                dpa_canton VARCHAR,
                dpa_descan VARCHAR,
                dpa_provin VARCHAR,
                dpa_despro VARCHAR,
                dpa_anio INTEGER,
                fcode VARCHAR,
                geometry_data JSONB,
                batch_id VARCHAR,
                ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """

        voice_measurements_table = """
            CREATE TABLE IF NOT EXISTS voice_measurements (
                measurement_id VARCHAR PRIMARY KEY,
                valid_from TIMESTAMP NOT NULL,
                valid_to TIMESTAMP,
                is_current INTEGER DEFAULT 1,
                batch_id VARCHAR,
                ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(mobile_measurements_table)
                cur.execute(geographic_table)
                cur.execute(voice_measurements_table)
            conn.commit()

    def _prepare_measurement_data(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Prepare mobile measurement data preserving original column names"""
        records = []
        batch_id = datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')

        for _, row in df.iterrows():
            # Create unique measurement ID
            id_components = [
                str(row.get('DatasourceId', '')),
                str(row.get('SessionId', '')),
                str(row.get('StartTime', ''))
            ]
            id_string = '_'.join(filter(None, id_components))
            measurement_id = hashlib.md5(id_string.encode()).hexdigest()

            # Create record with original column names
            record = {
                'measurement_id': measurement_id,
                'valid_from': datetime.datetime.utcnow(),
                'is_current': 1,
                'batch_id': batch_id
            }

            # Add all original columns, converting data types appropriately
            for col in df.columns:
                value = row[col]

                # Handle different data types
                if pd.isna(value):
                    record[col] = None
                elif isinstance(value, (pd.Timestamp, datetime.datetime)):
                    record[col] = value
                elif isinstance(value, pd.Timedelta):
                    record[col] = value.total_seconds() if pd.notna(value) else None
                elif isinstance(value, (int, float, str)):
                    record[col] = value
                else:
                    # Convert other types to string
                    record[col] = str(value)

            records.append(record)

        return records

    def _prepare_geographic_data(self, gdf: 'GeoDataFrame') -> List[Dict[str, Any]]:
        """Prepare geographic data with proper structuring"""
        records = []
        batch_id = datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')

        for _, row in gdf.iterrows():
            try:
                id_string = f"{row['DPA_DESPRO']}_{row['DPA_DESCAN']}_{row['DPA_DESPAR']}"
                region_id = hashlib.md5(id_string.encode()).hexdigest()

                if pd.notna(row['geometry']):
                    geometry_json = json.loads(gpd.GeoSeries([row['geometry']]).to_json())
                    geometry_data = geometry_json['features'][0]['geometry']
                else:
                    geometry_data = None

                record = {
                    'region_id': region_id,
                    'valid_from': datetime.datetime.utcnow(),
                    'is_current': 1,
                    'dpa_despar': row['DPA_DESPAR'] if pd.notna(row['DPA_DESPAR']) else None,
                    'dpa_canton': row['DPA_CANTON'] if pd.notna(row['DPA_CANTON']) else None,
                    'dpa_descan': row['DPA_DESCAN'] if pd.notna(row['DPA_DESCAN']) else None,
                    'dpa_provin': row['DPA_PROVIN'] if pd.notna(row['DPA_PROVIN']) else None,
                    'dpa_despro': row['DPA_DESPRO'] if pd.notna(row['DPA_DESPRO']) else None,
                    'dpa_anio': int(row['DPA_ANIO']) if pd.notna(row['DPA_ANIO']) else None,
                    'fcode': row['fcode'] if pd.notna(row['fcode']) else None,
                    'geometry_data': json.dumps(geometry_data) if geometry_data else None,
                    'batch_id': batch_id
                }
                records.append(record)
            except Exception as e:
                print(f"Error processing geographic record: {e}")
                continue

        return records

    def _prepare_voice_data(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Prepare voice measurement data preserving original column names"""
        records = []
        batch_id = datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')

        for _, row in df.iterrows():
            # Create unique measurement ID
            id_components = [
                str(row.get('DatasourceId', '')),
                str(row.get('CallIndex', '')),
                str(row.get('DialStartDateTime', ''))
            ]
            id_string = '_'.join(filter(None, id_components))
            measurement_id = hashlib.md5(id_string.encode()).hexdigest()

            # Create record with original column names
            record = {
                'measurement_id': measurement_id,
                'valid_from': datetime.datetime.utcnow(),
                'is_current': 1,
                'batch_id': batch_id
            }

            # Add all original columns, converting data types appropriately
            for col in df.columns:
                value = row[col]

                # Handle different data types
                if pd.isna(value):
                    record[col] = None
                elif isinstance(value, (pd.Timestamp, datetime.datetime)):
                    record[col] = value
                elif isinstance(value, pd.Timedelta):
                    record[col] = value.total_seconds() if pd.notna(value) else None
                elif isinstance(value, (int, float, str)):
                    record[col] = value
                else:
                    # Convert other types to string
                    record[col] = str(value)

            records.append(record)

        return records

    def upsert_measurements(self, df: pd.DataFrame):
        """Upsert mobile measurement data preserving original column structure"""

        # First, dynamically add columns to the table if they don't exist
        self._ensure_mobile_columns_exist(df)

        records = self._prepare_measurement_data(df)
        new_records = 0
        updated_records = 0
        skipped_records = 0

        # Get all column names except metadata columns
        data_columns = [col for col in df.columns]
        metadata_columns = ['measurement_id', 'valid_from', 'valid_to', 'is_current', 'batch_id', 'ingestion_timestamp']
        all_columns = metadata_columns + data_columns

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                for record in records:
                    try:
                        # Check if record exists and is_current
                        cur.execute("""
                            SELECT measurement_id FROM mobile_measurements 
                            WHERE measurement_id = %s AND is_current = 1
                        """, (record['measurement_id'],))

                        existing = cur.fetchone()

                        if existing:
                            # For simplicity, we'll just skip existing records
                            # In production, you might want to implement proper change detection
                            skipped_records += 1
                        else:
                            # Build dynamic INSERT query
                            columns = ', '.join([f'"{col}"' for col in all_columns])
                            placeholders = ', '.join(['%s'] * len(all_columns))

                            insert_query = f"""
                                INSERT INTO mobile_measurements ({columns})
                                VALUES ({placeholders})
                            """

                            # Prepare values in the same order as columns
                            values = [record.get(col) for col in all_columns]

                            cur.execute(insert_query, values)
                            new_records += 1

                    except Exception as e:
                        print(f"Error processing mobile record {record['measurement_id']}: {e}")
                        continue

                conn.commit()

        print(f"Processed {len(records)} mobile records:")
        print(f"- Inserted {new_records} new records")
        print(f"- Updated {updated_records} existing records")
        print(f"- Skipped {skipped_records} unchanged records")

    def should_insert_geographic_data(self) -> bool:
        """Check if geographic_regions table is empty"""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM geographic_regions")
                count = cur.fetchone()[0]
                return count == 0

    def upsert_geographic_data(self, gdf: 'GeoDataFrame'):
        """Insert geographic data only if table is empty"""
        if not self.should_insert_geographic_data():
            print("Geographic data already exists in database. Skipping insertion.")
            return

        records = self._prepare_geographic_data(gdf)

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                execute_batch(cur, """
                    INSERT INTO geographic_regions (
                        region_id, valid_from, is_current, dpa_despar,
                        dpa_canton, dpa_descan, dpa_provin, dpa_despro,
                        dpa_anio, fcode, geometry_data, batch_id
                    ) VALUES (
                        %(region_id)s, %(valid_from)s, %(is_current)s,
                        %(dpa_despar)s, %(dpa_canton)s, %(dpa_descan)s,
                        %(dpa_provin)s, %(dpa_despro)s, %(dpa_anio)s,
                        %(fcode)s, %(geometry_data)s::jsonb, %(batch_id)s
                    )
                """, records, page_size=1000)
            conn.commit()

    def upsert_voice_measurements(self, df: pd.DataFrame):
        """Upsert voice measurement data preserving original column structure"""

        # First, dynamically add columns to the table if they don't exist
        self._ensure_voice_columns_exist(df)

        records = self._prepare_voice_data(df)
        new_records = 0
        updated_records = 0
        skipped_records = 0

        # Get all column names except metadata columns
        data_columns = [col for col in df.columns]
        metadata_columns = ['measurement_id', 'valid_from', 'valid_to', 'is_current', 'batch_id', 'ingestion_timestamp']
        all_columns = metadata_columns + data_columns

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                for record in records:
                    try:
                        # Check if record exists and is_current
                        cur.execute("""
                            SELECT measurement_id FROM voice_measurements 
                            WHERE measurement_id = %s AND is_current = 1
                        """, (record['measurement_id'],))

                        existing = cur.fetchone()

                        if existing:
                            # For simplicity, we'll just skip existing records
                            # In production, you might want to implement proper change detection
                            skipped_records += 1
                        else:
                            # Build dynamic INSERT query
                            columns = ', '.join([f'"{col}"' for col in all_columns])
                            placeholders = ', '.join(['%s'] * len(all_columns))

                            insert_query = f"""
                                INSERT INTO voice_measurements ({columns})
                                VALUES ({placeholders})
                            """

                            # Prepare values in the same order as columns
                            values = [record.get(col) for col in all_columns]

                            cur.execute(insert_query, values)
                            new_records += 1

                    except Exception as e:
                        print(f"Error processing voice record {record['measurement_id']}: {e}")
                        continue

                conn.commit()

        print(f"Processed {len(records)} voice records:")
        print(f"- Inserted {new_records} new records")
        print(f"- Updated {updated_records} existing records")
        print(f"- Skipped {skipped_records} unchanged records")

    def _ensure_mobile_columns_exist(self, df: pd.DataFrame):
        """Dynamically add columns to mobile_measurements table if they don't exist"""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                # Get existing columns
                cur.execute("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = 'mobile_measurements'
                """)
                existing_columns = {row[0]: row[1] for row in cur.fetchall()}

                # Add missing columns
                for col in df.columns:
                    if col not in existing_columns:
                        # Determine PostgreSQL data type based on pandas dtype
                        pg_type = self._get_postgres_type(df[col].dtype)

                        try:
                            alter_query = f'ALTER TABLE mobile_measurements ADD COLUMN "{col}" {pg_type}'
                            cur.execute(alter_query)
                            print(f"Added mobile column: {col} ({pg_type})")
                        except Exception as e:
                            print(f"Warning: Could not add mobile column {col}: {e}")

                conn.commit()

    def _ensure_voice_columns_exist(self, df: pd.DataFrame):
        """Dynamically add columns to voice_measurements table if they don't exist"""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                # Get existing columns
                cur.execute("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = 'voice_measurements'
                """)
                existing_columns = {row[0]: row[1] for row in cur.fetchall()}

                # Add missing columns
                for col in df.columns:
                    if col not in existing_columns:
                        # Determine PostgreSQL data type based on pandas dtype
                        pg_type = self._get_postgres_type(df[col].dtype)

                        try:
                            alter_query = f'ALTER TABLE voice_measurements ADD COLUMN "{col}" {pg_type}'
                            cur.execute(alter_query)
                            print(f"Added voice column: {col} ({pg_type})")
                        except Exception as e:
                            print(f"Warning: Could not add voice column {col}: {e}")

                conn.commit()

    def _get_postgres_type(self, pandas_dtype):
        """Convert pandas dtype to PostgreSQL type"""
        dtype_str = str(pandas_dtype)

        if 'int' in dtype_str:
            return 'BIGINT'
        elif 'float' in dtype_str:
            return 'DOUBLE PRECISION'
        elif 'datetime' in dtype_str:
            return 'TIMESTAMP'
        elif 'timedelta' in dtype_str:
            return 'DOUBLE PRECISION'  # Store as seconds
        elif 'bool' in dtype_str:
            return 'BOOLEAN'
        else:
            return 'TEXT'  # Default to text for everything else


def get_postgres_handler(host: str, port: str, database: str, user: str, password: str) -> PostgresDataHandler:
    """Create a PostgresDataHandler instance"""
    connection_params = {
        'host': host,
        'port': port,
        'database': database,
        'user': user,
        'password': password
    }
    return PostgresDataHandler(connection_params)
