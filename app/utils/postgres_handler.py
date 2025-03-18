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
        measurements_table = """
            CREATE TABLE IF NOT EXISTS mobile_measurements (
                measurement_id VARCHAR PRIMARY KEY,
                valid_from TIMESTAMP NOT NULL,
                valid_to TIMESTAMP,
                is_current INTEGER DEFAULT 1,
                datasource_id VARCHAR,
                session_id VARCHAR,
                session_type VARCHAR,
                device_info JSONB,
                location_data JSONB,
                measurement_data JSONB,
                radio_info JSONB,
                operator_info JSONB,
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

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(measurements_table)
                cur.execute(geographic_table)
            conn.commit()

    def _prepare_measurement_data(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Prepare measurement data with proper structuring"""
        records = []
        batch_id = datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')

        for _, row in df.iterrows():
            device_info = {
                'device_name': row.get('Device') if pd.notna(row.get('Device')) else None,
                'imei': row.get('IMEI') if pd.notna(row.get('IMEI')) else None,
                'imsi': row.get('IMSI') if pd.notna(row.get('IMSI')) else None
            }

            location_data = {
                'start_location': {
                    'latitude': float(row.get('StartLatitude')) if pd.notna(row.get('StartLatitude')) else None,
                    'longitude': float(row.get('StartLongitude')) if pd.notna(row.get('StartLongitude')) else None
                },
                'end_location': {
                    'latitude': float(row.get('EndLatitude')) if pd.notna(row.get('EndLatitude')) else None,
                    'longitude': float(row.get('EndLongitude')) if pd.notna(row.get('EndLongitude')) else None
                }
            }

            measurement_data = {
                'start_time': row.get('StartTime').isoformat() if pd.notna(row.get('StartTime')) else None,
                'end_time': row.get('EndTime').isoformat() if pd.notna(row.get('EndTime')) else None,
                'throughput_mbps': float(row.get('ThroughputMbps')) if pd.notna(row.get('ThroughputMbps')) else None,
                'end_file_size': float(row.get('EndFileSize')) if pd.notna(row.get('EndFileSize')) else None
            }

            radio_info = {
                'start_technology': row.get('StartRadioTechnology') if pd.notna(
                    row.get('StartRadioTechnology')) else None,
                'end_technology': row.get('EndRadioTechnology') if pd.notna(row.get('EndRadioTechnology')) else None
            }

            operator_info = {
                'sim_operator': row.get('SimOperator') if pd.notna(row.get('SimOperator')) else None,
                'czo': row.get('CZO') if pd.notna(row.get('CZO')) else None
            }

            id_string = f"{row.get('DatasourceId')}_{row.get('SessionId')}_{row.get('StartTime')}"
            measurement_id = hashlib.md5(id_string.encode()).hexdigest()

            record = {
                'measurement_id': measurement_id,
                'valid_from': datetime.datetime.utcnow(),
                'is_current': 1,
                'datasource_id': str(row.get('DatasourceId')) if pd.notna(row.get('DatasourceId')) else None,
                'session_id': str(row.get('SessionId')) if pd.notna(row.get('SessionId')) else None,
                'session_type': row.get('SessionType') if pd.notna(row.get('SessionType')) else None,
                'device_info': json.dumps(device_info),
                'location_data': json.dumps(location_data),
                'measurement_data': json.dumps(measurement_data),
                'radio_info': json.dumps(radio_info),
                'operator_info': json.dumps(operator_info),
                'batch_id': batch_id
            }
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

    def upsert_measurements(self, df: pd.DataFrame):
        """Upsert measurement data with proper SCD Type 2 handling"""
        records = self._prepare_measurement_data(df)
        new_records = 0
        updated_records = 0
        skipped_records = 0

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                for record in records:
                    try:
                        # Check if record exists and is_current
                        cur.execute("""
                            SELECT measurement_id, device_info, location_data, 
                                   measurement_data, radio_info, operator_info
                            FROM mobile_measurements 
                            WHERE measurement_id = %(measurement_id)s AND is_current = 1
                        """, {'measurement_id': record['measurement_id']})

                        existing = cur.fetchone()

                        if existing:
                            # Compare if data has changed
                            existing_data = {
                                'device_info': existing[1],
                                'location_data': existing[2],
                                'measurement_data': existing[3],
                                'radio_info': existing[4],
                                'operator_info': existing[5]
                            }

                            new_data = {
                                'device_info': json.loads(record['device_info']),
                                'location_data': json.loads(record['location_data']),
                                'measurement_data': json.loads(record['measurement_data']),
                                'radio_info': json.loads(record['radio_info']),
                                'operator_info': json.loads(record['operator_info'])
                            }

                            if existing_data != new_data:
                                # Update the existing record
                                cur.execute("""
                                    UPDATE mobile_measurements 
                                    SET is_current = 0, valid_to = %(valid_from)s
                                    WHERE measurement_id = %(measurement_id)s 
                                    AND is_current = 1
                                """, record)

                                # Insert new version
                                cur.execute("""
                                    INSERT INTO mobile_measurements (
                                        measurement_id, valid_from, is_current, datasource_id,
                                        session_id, session_type, device_info, location_data,
                                        measurement_data, radio_info, operator_info, batch_id
                                    ) VALUES (
                                        %(measurement_id)s, %(valid_from)s, %(is_current)s,
                                        %(datasource_id)s, %(session_id)s, %(session_type)s,
                                        %(device_info)s::jsonb, %(location_data)s::jsonb,
                                        %(measurement_data)s::jsonb, %(radio_info)s::jsonb,
                                        %(operator_info)s::jsonb, %(batch_id)s
                                    )
                                """, record)
                                updated_records += 1
                            else:
                                skipped_records += 1
                        else:
                            # Insert new record
                            cur.execute("""
                                INSERT INTO mobile_measurements (
                                    measurement_id, valid_from, is_current, datasource_id,
                                    session_id, session_type, device_info, location_data,
                                    measurement_data, radio_info, operator_info, batch_id
                                ) VALUES (
                                    %(measurement_id)s, %(valid_from)s, %(is_current)s,
                                    %(datasource_id)s, %(session_id)s, %(session_type)s,
                                    %(device_info)s::jsonb, %(location_data)s::jsonb,
                                    %(measurement_data)s::jsonb, %(radio_info)s::jsonb,
                                    %(operator_info)s::jsonb, %(batch_id)s
                                )
                            """, record)
                            new_records += 1

                    except Exception as e:
                        print(f"Error processing record {record['measurement_id']}: {e}")
                        continue

                conn.commit()

        print(f"Processed {len(records)} records:")
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
