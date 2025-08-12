import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2 import sql
import datetime
import hashlib
import json
import gc
from typing import Dict, List, Any
import geopandas as gpd


class PostgresDataHandler:
    def __init__(self, connection_params: Dict[str, str]):
        """Initialize PostgreSQL connection parameters"""
        self.connection_params = connection_params
        # Test connection on initialization
        self._test_connection()

    def _test_connection(self):
        """Test database connection on initialization"""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            print("✅ PostgreSQL connection validated successfully")
        except Exception as e:
            raise ConnectionError(f"❌ Failed to connect to PostgreSQL: {e}")

    def _get_connection(self):
        """Create and return a new database connection with proper error handling"""
        try:
            conn = psycopg2.connect(
                dbname=self.connection_params['database'],
                user=self.connection_params['user'],
                password=self.connection_params['password'],
                host=self.connection_params['host'],
                port=self.connection_params['port']
            )
            conn.autocommit = False  # Ensure explicit transaction control
            return conn
        except psycopg2.Error as e:
            raise ConnectionError(f"Failed to connect to PostgreSQL: {e}")

    def create_tables(self):
        """Create tables if they don't exist with proper indexes"""
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

        # Index creation queries
        mobile_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_mobile_batch_id ON mobile_measurements(batch_id);",
            "CREATE INDEX IF NOT EXISTS idx_mobile_valid_from ON mobile_measurements(valid_from);",
            "CREATE INDEX IF NOT EXISTS idx_mobile_is_current ON mobile_measurements(is_current);",
            "CREATE INDEX IF NOT EXISTS idx_mobile_ingestion ON mobile_measurements(ingestion_timestamp);"
        ]

        voice_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_voice_batch_id ON voice_measurements(batch_id);",
            "CREATE INDEX IF NOT EXISTS idx_voice_valid_from ON voice_measurements(valid_from);",
            "CREATE INDEX IF NOT EXISTS idx_voice_is_current ON voice_measurements(is_current);",
            "CREATE INDEX IF NOT EXISTS idx_voice_ingestion ON voice_measurements(ingestion_timestamp);"
        ]

        geographic_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_geo_batch_id ON geographic_regions(batch_id);",
            "CREATE INDEX IF NOT EXISTS idx_geo_valid_from ON geographic_regions(valid_from);",
            "CREATE INDEX IF NOT EXISTS idx_geo_is_current ON geographic_regions(is_current);",
            "CREATE INDEX IF NOT EXISTS idx_geo_despro ON geographic_regions(dpa_despro);",
            "CREATE INDEX IF NOT EXISTS idx_geo_descan ON geographic_regions(dpa_descan);"
        ]

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    # Create tables
                    cur.execute(mobile_measurements_table)
                    cur.execute(geographic_table)
                    cur.execute(voice_measurements_table)

                    # Create indexes
                    for index_query in mobile_indexes + voice_indexes + geographic_indexes:
                        try:
                            cur.execute(index_query)
                        except psycopg2.Error as e:
                            print(f"Warning: Could not create index: {e}")

                conn.commit()
                print("✅ Tables and indexes created/verified successfully")
        except Exception as e:
            print(f"❌ Error creating tables: {e}")
            raise

    def _generate_deterministic_id(self, row: pd.Series, id_fields: List[str]) -> str:
        """Generate deterministic ID using only invariant fields"""
        id_components = []
        for field in id_fields:
            value = row.get(field, '')
            if pd.notna(value) and str(value).strip():
                id_components.append(str(value).strip())

        if not id_components:
            raise ValueError(f"No valid ID components found in fields: {id_fields}")

        id_string = '_'.join(id_components)
        return hashlib.md5(id_string.encode()).hexdigest()

    def _prepare_measurement_data(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Prepare mobile measurement data with 1:1 guarantee"""
        return self._prepare_data_with_guarantee(df, "mobile")

    def _prepare_voice_data(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Prepare voice measurement data with 1:1 guarantee"""
        return self._prepare_data_with_guarantee(df, "voice")

    def _prepare_geographic_data(self, gdf: 'GeoDataFrame') -> List[Dict[str, Any]]:
        """Prepare geographic data with proper error handling"""
        records = []
        batch_id = datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')

        for _, row in gdf.iterrows():
            try:
                # Generate deterministic ID
                id_fields = ['DPA_DESPRO', 'DPA_DESCAN', 'DPA_DESPAR']
                region_id = self._generate_deterministic_id(row, id_fields)

                # Process geometry
                geometry_data = None
                if pd.notna(row['geometry']):
                    try:
                        geometry_json = json.loads(gpd.GeoSeries([row['geometry']]).to_json())
                        geometry_data = geometry_json['features'][0]['geometry']
                    except Exception as e:
                        print(f"Warning: Could not process geometry: {e}")

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
                print(f"❌ Error processing geographic record: {e}")
                continue

        return records

    def _prepare_data_with_guarantee(self, df: pd.DataFrame, data_type: str) -> List[Dict[str, Any]]:
        """Guarantee 1:1 record mapping or fail explicitly"""

        initial_count = len(df)
        records = []
        failed_records = []

        print(f"🔒 GUARANTEE MODE: Processing {initial_count:,} {data_type} records with 1:1 guarantee")

        # Select appropriate preparation method
        if data_type == "mobile":
            id_fields = ['DatasourceId', 'SessionId', 'StartLatitude', 'StartLongitude', 'EndLatitude', 'EndLongitude']
        elif data_type == "voice":
            id_fields = ['DatasourceId', 'CallIndex', 'IMSI', 'IMEI', 'EndLatitude', 'EndLongitude', 'SentenceIndex']
        else:
            raise ValueError(f"Unknown data type: {data_type}")

        batch_id = datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')

        for i, (_, row) in enumerate(df.iterrows()):
            try:
                # Generate deterministic ID
                measurement_id = self._generate_deterministic_id(row, id_fields)

                # Create record with metadata
                record = {
                    'measurement_id': measurement_id,
                    'valid_from': datetime.datetime.utcnow(),
                    'is_current': 1,
                    'batch_id': batch_id
                }

                # Add all original columns with proper type conversion
                for col in df.columns:
                    value = row[col]
                    record[col] = self._convert_value_for_postgres(value)

                records.append(record)

            except Exception as e:
                failed_records.append((i, str(e)))
                print(f"❌ Failed to prepare {data_type} record {i}: {e}")

        final_count = len(records)

        # GUARANTEE: Either process ALL or fail completely
        if final_count != initial_count:
            error_msg = f"""
❌ GUARANTEE FAILED: Record count mismatch for {data_type} data!
- DataFrame input: {initial_count:,} records
- Prepared for DB: {final_count:,} records  
- Failed records: {len(failed_records)}
- Success rate: {(final_count / initial_count * 100):.1f}%
- First 5 failures: {failed_records[:5]}
"""
            print(error_msg)
            raise ValueError(f"GUARANTEE VIOLATION: {data_type} records {final_count}/{initial_count}")

        print(f"✅ GUARANTEE SATISFIED: {final_count:,} {data_type} records prepared (100% success)")
        return records

    def _convert_value_for_postgres(self, value):
        """Convert pandas values to PostgreSQL-compatible types"""
        if pd.isna(value):
            return None
        elif isinstance(value, (pd.Timestamp, datetime.datetime)):
            return value
        elif isinstance(value, pd.Timedelta):
            return value.total_seconds() if pd.notna(value) else None
        elif isinstance(value, (int, float)):
            return value
        elif isinstance(value, str):
            return value
        else:
            return str(value)

    def upsert_measurements(self, df: pd.DataFrame, chunk_size: int = 1000):
        """Upsert mobile measurement data with robust transaction handling"""
        try:
            # First, dynamically add columns to the table if they don't exist
            self._ensure_mobile_columns_exist(df)

            # Process in chunks to manage memory
            total_records = len(df)
            total_new_records = 0
            total_skipped_records = 0

            print(f"Processing {total_records} mobile records in chunks of {chunk_size}")

            # Get all column names
            data_columns = [col for col in df.columns]
            metadata_columns = ['measurement_id', 'valid_from', 'valid_to', 'is_current', 'batch_id',
                                'ingestion_timestamp']
            all_columns = metadata_columns + data_columns

            # Process DataFrame in chunks
            for chunk_start in range(0, total_records, chunk_size):
                chunk_end = min(chunk_start + chunk_size, total_records)
                chunk_df = df.iloc[chunk_start:chunk_end]
                chunk_num = chunk_start // chunk_size + 1
                total_chunks = (total_records + chunk_size - 1) // chunk_size

                print(f"Processing mobile chunk {chunk_num}/{total_chunks}: rows {chunk_start} to {chunk_end}")

                # Prepare records for this chunk
                records = self._prepare_measurement_data(chunk_df)
                new_records = 0
                skipped_records = 0

                # Use transaction with savepoints for this chunk
                try:
                    with self._get_connection() as conn:
                        with conn.cursor() as cur:
                            for i, record in enumerate(records):
                                try:
                                    # Create savepoint for this record
                                    savepoint_name = f"sp_{i}"
                                    cur.execute(f"SAVEPOINT {savepoint_name}")

                                    # Check if record exists
                                    cur.execute("""
                                        SELECT measurement_id FROM mobile_measurements 
                                        WHERE measurement_id = %s AND is_current = 1
                                    """, (record['measurement_id'],))

                                    existing = cur.fetchone()

                                    if existing:
                                        skipped_records += 1
                                        cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
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
                                        cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")

                                except Exception as e:
                                    print(f"❌ Error processing mobile record {i}: {e}")
                                    cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                                    continue

                            # Commit the entire chunk
                            conn.commit()

                except Exception as e:
                    print(f"❌ Error processing mobile chunk {chunk_num}: {e}")
                    continue

                # Update totals
                total_new_records += new_records
                total_skipped_records += skipped_records

                print(f"  Chunk {chunk_num}: Inserted {new_records}, Skipped {skipped_records}")

                # Clear chunk memory
                del chunk_df, records
                gc.collect()

            print(f"\n✅ Completed processing {total_records} mobile records:")
            print(f"- Inserted {total_new_records} new records")
            print(f"- Skipped {total_skipped_records} duplicate records")

        except Exception as e:
            print(f"❌ Critical error in upsert_measurements: {e}")
            raise

    def should_insert_geographic_data(self) -> bool:
        """Check if geographic_regions table is empty"""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM geographic_regions")
                    count = cur.fetchone()[0]
                    return count == 0
        except Exception as e:
            print(f"❌ Error checking geographic data: {e}")
            return False

    def upsert_geographic_data(self, gdf: 'GeoDataFrame'):
        """Insert geographic data only if table is empty"""
        if not self.should_insert_geographic_data():
            print("Geographic data already exists in database. Skipping insertion.")
            return

        try:
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

            print(f"✅ Inserted {len(records)} geographic records")

        except Exception as e:
            print(f"❌ Error inserting geographic data: {e}")
            raise

    def upsert_voice_measurements(self, df: pd.DataFrame, chunk_size: int = 1000):
        """Upsert voice measurement data with robust transaction handling"""
        try:
            # First, dynamically add columns to the table if they don't exist
            self._ensure_voice_columns_exist(df)

            # Process in chunks to manage memory
            total_records = len(df)
            total_new_records = 0
            total_skipped_records = 0

            print(f"Processing {total_records} voice records in chunks of {chunk_size}")

            # Get all column names
            data_columns = [col for col in df.columns]
            metadata_columns = ['measurement_id', 'valid_from', 'valid_to', 'is_current', 'batch_id',
                                'ingestion_timestamp']
            all_columns = metadata_columns + data_columns

            # Process DataFrame in chunks
            for chunk_start in range(0, total_records, chunk_size):
                chunk_end = min(chunk_start + chunk_size, total_records)
                chunk_df = df.iloc[chunk_start:chunk_end]
                chunk_num = chunk_start // chunk_size + 1
                total_chunks = (total_records + chunk_size - 1) // chunk_size

                print(f"Processing voice chunk {chunk_num}/{total_chunks}: rows {chunk_start} to {chunk_end}")

                # Prepare records for this chunk
                records = self._prepare_voice_data(chunk_df)
                new_records = 0
                skipped_records = 0

                # Use transaction with savepoints for this chunk
                try:
                    with self._get_connection() as conn:
                        with conn.cursor() as cur:
                            for i, record in enumerate(records):
                                try:
                                    # Create savepoint for this record
                                    savepoint_name = f"sp_{i}"
                                    cur.execute(f"SAVEPOINT {savepoint_name}")

                                    # Check if record exists
                                    cur.execute("""
                                        SELECT measurement_id FROM voice_measurements 
                                        WHERE measurement_id = %s AND is_current = 1
                                    """, (record['measurement_id'],))

                                    existing = cur.fetchone()

                                    if existing:
                                        skipped_records += 1
                                        cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
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
                                        cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")

                                except Exception as e:
                                    print(f"❌ Error processing voice record {i}: {e}")
                                    cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                                    continue

                            # Commit the entire chunk
                            conn.commit()

                except Exception as e:
                    print(f"❌ Error processing voice chunk {chunk_num}: {e}")
                    continue

                # Update totals
                total_new_records += new_records
                total_skipped_records += skipped_records

                print(f"  Chunk {chunk_num}: Inserted {new_records}, Skipped {skipped_records}")

                # Clear chunk memory
                del chunk_df, records
                gc.collect()

            print(f"\n✅ Completed processing {total_records} voice records:")
            print(f"- Inserted {total_new_records} new records")
            print(f"- Skipped {total_skipped_records} duplicate records")

        except Exception as e:
            print(f"❌ Critical error in upsert_voice_measurements: {e}")
            raise

    def _ensure_mobile_columns_exist(self, df: pd.DataFrame):
        """Dynamically add columns to mobile_measurements table with robust validation"""
        critical_columns = ['DatasourceId', 'SessionId', 'StartLatitude', 'StartLongitude', 'EndLatitude',
                            'EndLongitude']

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    # Get existing columns
                    cur.execute("""
                        SELECT column_name, data_type 
                        FROM information_schema.columns 
                        WHERE table_name = 'mobile_measurements'
                    """)
                    existing_columns = {row[0]: row[1] for row in cur.fetchall()}

                    # Add missing columns with individual transactions
                    for col in df.columns:
                        if col not in existing_columns:
                            # Use separate transaction for each column to prevent cascading failures
                            with self._get_connection() as col_conn:
                                with col_conn.cursor() as col_cur:
                                    try:
                                        # Determine PostgreSQL data type with validation
                                        pg_type = self._get_postgres_type_safe(df[col].dtype, df[col])

                                        # Use quoted column name to handle special characters
                                        alter_query = sql.SQL(
                                            'ALTER TABLE mobile_measurements ADD COLUMN {} {}').format(
                                            sql.Identifier(col),
                                            sql.SQL(pg_type)
                                        )
                                        col_cur.execute(alter_query)
                                        col_conn.commit()
                                        print(f"✅ Added mobile column: {col} ({pg_type})")

                                    except Exception as e:
                                        col_conn.rollback()
                                        if col in critical_columns:
                                            print(f"❌ CRITICAL: Failed to add mobile column {col}: {e}")
                                            raise ValueError(f"Critical column {col} could not be created: {e}")
                                        else:
                                            print(f"⚠️  Warning: Could not add mobile column {col}: {e}")

                    # Verify critical columns exist
                    cur.execute("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = 'mobile_measurements'
                    """)
                    final_columns = {row[0] for row in cur.fetchall()}

                    missing_critical = [col for col in critical_columns if col not in final_columns]
                    if missing_critical:
                        raise ValueError(f"❌ CRITICAL: Missing essential mobile columns: {missing_critical}")

                    print(f"✅ Mobile table schema validated - {len(final_columns)} columns ready")

        except Exception as e:
            print(f"❌ Error ensuring mobile columns exist: {e}")
            raise

    def _ensure_voice_columns_exist(self, df: pd.DataFrame):
        """Dynamically add columns to voice_measurements table with robust validation"""
        critical_columns = ['DatasourceId', 'CallIndex', 'IMSI', 'IMEI', 'EndLatitude', 'EndLongitude']

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    # Get existing columns
                    cur.execute("""
                        SELECT column_name, data_type 
                        FROM information_schema.columns 
                        WHERE table_name = 'voice_measurements'
                    """)
                    existing_columns = {row[0]: row[1] for row in cur.fetchall()}

                    # Add missing columns with individual transactions
                    for col in df.columns:
                        if col not in existing_columns:
                            # Use separate transaction for each column to prevent cascading failures
                            with self._get_connection() as col_conn:
                                with col_conn.cursor() as col_cur:
                                    try:
                                        # Determine PostgreSQL data type with validation
                                        pg_type = self._get_postgres_type_safe(df[col].dtype, df[col])

                                        # Use quoted column name to handle special characters
                                        alter_query = sql.SQL('ALTER TABLE voice_measurements ADD COLUMN {} {}').format(
                                            sql.Identifier(col),
                                            sql.SQL(pg_type)
                                        )
                                        col_cur.execute(alter_query)
                                        col_conn.commit()
                                        print(f"✅ Added voice column: {col} ({pg_type})")

                                    except Exception as e:
                                        col_conn.rollback()
                                        if col in critical_columns:
                                            print(f"❌ CRITICAL: Failed to add voice column {col}: {e}")
                                            raise ValueError(f"Critical column {col} could not be created: {e}")
                                        else:
                                            print(f"⚠️  Warning: Could not add voice column {col}: {e}")

                    # Verify critical columns exist
                    cur.execute("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = 'voice_measurements'
                    """)
                    final_columns = {row[0] for row in cur.fetchall()}

                    missing_critical = [col for col in critical_columns if col not in final_columns]
                    if missing_critical:
                        raise ValueError(f"❌ CRITICAL: Missing essential voice columns: {missing_critical}")

                    print(f"✅ Voice table schema validated - {len(final_columns)} columns ready")

        except Exception as e:
            print(f"❌ Error ensuring voice columns exist: {e}")
            raise

    def _get_postgres_type_safe(self, pandas_dtype, column_sample=None):
        """Convert pandas dtype to optimal PostgreSQL type with safe validation"""
        dtype_str = str(pandas_dtype).lower()

        # Integer types - be more specific about size
        if 'int8' in dtype_str:
            return 'SMALLINT'
        elif 'int16' in dtype_str:
            return 'SMALLINT'
        elif 'int32' in dtype_str:
            return 'INTEGER'
        elif 'int64' in dtype_str or 'int' in dtype_str:
            return 'BIGINT'

        # Float types
        elif 'float32' in dtype_str:
            return 'REAL'
        elif 'float64' in dtype_str or 'float' in dtype_str:
            return 'DOUBLE PRECISION'

        # Date/time types
        elif 'datetime' in dtype_str:
            return 'TIMESTAMP'
        elif 'timedelta' in dtype_str:
            return 'DOUBLE PRECISION'  # Store as seconds

        # Boolean
        elif 'bool' in dtype_str:
            return 'BOOLEAN'

        # String/object - try to optimize based on content with SAFE validation
        elif 'object' in dtype_str or 'string' in dtype_str:
            if column_sample is not None:
                # Sample non-null values to determine optimal string type
                non_null_values = column_sample.dropna().astype(str)
                if len(non_null_values) > 0:
                    max_length = non_null_values.str.len().max()

                    # SAFE LENGTH VALIDATION - prevent VARCHAR(0) errors
                    if max_length <= 0:
                        return 'TEXT'  # Fallback for empty/null columns
                    elif max_length <= 255:
                        # Ensure minimum length of 1 and reasonable buffer
                        safe_length = max(max_length * 2, 10)
                        return f'VARCHAR({min(safe_length, 255)})'
                    elif max_length <= 1000:
                        return 'VARCHAR(1000)'
                    else:
                        return 'TEXT'
                else:
                    return 'TEXT'  # No valid samples
            return 'TEXT'  # No sample provided

        # Default fallback
        else:
            return 'TEXT'

    def _get_postgres_type(self, pandas_dtype, column_sample=None):
        """Legacy method - redirects to safe version"""
        return self._get_postgres_type_safe(pandas_dtype, column_sample)


def get_postgres_handler(host: str, port: str, database: str, user: str, password: str) -> PostgresDataHandler:
    """Create a PostgresDataHandler instance with connection validation"""
    connection_params = {
        'host': host,
        'port': port,
        'database': database,
        'user': user,
        'password': password
    }
    return PostgresDataHandler(connection_params)
