import psycopg2
import json
from shapely.geometry import Point, shape
from typing import Dict


class SpatialMapper:
    def __init__(self, connection_params: Dict[str, str]):
        self.connection_params = connection_params

    def _get_connection(self):
        return psycopg2.connect(**self.connection_params)

    def process_and_map_locations(self):
        # Primero verificar si hay nuevos registros
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(m.measurement_id)
                    FROM mobile_measurements m
                    LEFT JOIN location_mapping lm ON m.measurement_id = lm.measurement_id
                    WHERE m.is_current = 1 AND lm.measurement_id IS NULL
                """)
                new_records = cur.fetchone()[0]

        if new_records == 0:
            print("No hay nuevos registros para procesar")
            return 0

        print(f"Encontrados {new_records} nuevos registros para procesar")

        BATCH_SIZE = 1000
        processed = 0
        ignored = 0

        print("Obteniendo regiones...")
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT region_id, geometry_data 
                    FROM geographic_regions 
                    WHERE is_current = 1
                """)
                regions = cur.fetchall()
                region_geometries = {
                    r[0]: shape(r[1] if isinstance(r[1], dict) else json.loads(r[1]))
                    for r in regions
                }

        print(f"Procesando mediciones...")
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT m.measurement_id, m.location_data 
                    FROM mobile_measurements m
                    LEFT JOIN location_mapping lm ON m.measurement_id = lm.measurement_id
                    WHERE m.is_current = 1 AND lm.measurement_id IS NULL
                """)

                mapping_records = []
                while True:
                    records = cur.fetchmany(BATCH_SIZE)
                    if not records:
                        break

                    batch_mappings = []
                    for record in records:
                        measurement_id, location_data = record
                        try:
                            if isinstance(location_data, str):
                                location_data = json.loads(location_data)

                            start_point = Point(
                                location_data['start_location']['longitude'],
                                location_data['start_location']['latitude']
                            )
                            end_point = Point(
                                location_data['end_location']['longitude'],
                                location_data['end_location']['latitude']
                            )

                            found_region = False
                            for region_id, region_geom in region_geometries.items():
                                if start_point.within(region_geom) and end_point.within(region_geom):
                                    batch_mappings.append({
                                        'measurement_id': measurement_id,
                                        'region_id': region_id,
                                        'location_type': 'both'
                                    })
                                    found_region = True
                                    break

                            if not found_region:
                                ignored += 1
                                print(f"Registro {measurement_id} ignorado: puntos fuera de todas las regiones")

                        except (KeyError, json.JSONDecodeError) as e:
                            ignored += 1
                            print(f"Error procesando registro {measurement_id}: {str(e)}")

                    processed += len(records)
                    print(f"Procesados {processed} registros...")

                    if batch_mappings:
                        with conn.cursor() as insert_cur:
                            insert_cur.executemany("""
                                INSERT INTO location_mapping (measurement_id, region_id, location_type)
                                VALUES (%(measurement_id)s, %(region_id)s, %(location_type)s)
                                ON CONFLICT DO NOTHING
                            """, batch_mappings)
                        conn.commit()
                        mapping_records.extend(batch_mappings)

        print(f"\nTotal procesados: {processed}")
        print(f"Total ignorados: {ignored}")
        print(f"Total mappings creados: {len(mapping_records)}")
        return len(mapping_records)

    def create_mapping_table(self):
        mapping_table = """
            CREATE TABLE IF NOT EXISTS location_mapping (
                measurement_id VARCHAR,
                region_id VARCHAR,
                location_type VARCHAR,
                PRIMARY KEY (measurement_id, region_id, location_type),
                FOREIGN KEY (measurement_id) REFERENCES mobile_measurements(measurement_id),
                FOREIGN KEY (region_id) REFERENCES geographic_regions(region_id)
            );
            CREATE INDEX IF NOT EXISTS idx_location_mapping_measurement ON location_mapping(measurement_id);
            CREATE INDEX IF NOT EXISTS idx_location_mapping_region ON location_mapping(region_id);
            CREATE INDEX IF NOT EXISTS idx_location_mapping_type ON location_mapping(location_type);
        """
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(mapping_table)
            conn.commit()


def get_spatial_mapper(host: str, port: str, database: str, user: str, password: str):
    connection_params = {
        'host': host,
        'port': port,
        'database': database,
        'user': user,
        'password': password
    }
    return SpatialMapper(connection_params)
