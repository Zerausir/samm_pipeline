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
        """Process and map mobile measurement locations"""
        return self._process_locations('mobile_measurements', 'mobile')

    def process_and_map_voice_locations(self):
        """Process and map voice measurement locations"""
        return self._process_locations('voice_measurements', 'voice')

    def _process_locations(self, table_name: str, data_type: str):
        """Generic method to process locations for any measurement table"""
        # Check for new records
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT COUNT(m.measurement_id)
                    FROM {table_name} m
                    LEFT JOIN location_mapping lm ON m.measurement_id = lm.measurement_id
                    WHERE m.is_current = 1 AND lm.measurement_id IS NULL
                """)
                new_records = cur.fetchone()[0]

        if new_records == 0:
            print(f"No hay nuevos registros de {data_type} para procesar")
            return 0

        print(f"Encontrados {new_records} nuevos registros de {data_type} para procesar")

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

                # IMPROVED DEBUG: Check geometry loading
                region_geometries = {}
                valid_regions = 0
                for r in regions:
                    try:
                        if r[1]:
                            geom_data = r[1] if isinstance(r[1], dict) else json.loads(r[1])
                            region_geometries[r[0]] = shape(geom_data)
                            valid_regions += 1
                    except Exception as e:
                        print(f"Error loading geometry for region {r[0]}: {e}")

                print(f"Loaded {valid_regions} valid region geometries out of {len(regions)} total regions")

        print(f"Procesando mediciones de {data_type}...")
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                # Build query based on data type to get coordinates directly
                if data_type == 'mobile':
                    query = f"""
                        SELECT m.measurement_id, 
                               m."StartLatitude", m."StartLongitude",
                               m."EndLatitude", m."EndLongitude"
                        FROM {table_name} m
                        LEFT JOIN location_mapping lm ON m.measurement_id = lm.measurement_id
                        WHERE m.is_current = 1 AND lm.measurement_id IS NULL
                    """
                elif data_type == 'voice':
                    query = f"""
                        SELECT m.measurement_id, 
                               m."StartLatitude", m."StartLongitude",
                               m."EndLatitude", m."EndLongitude",
                               m."Latitude", m."Longitude"
                        FROM {table_name} m
                        LEFT JOIN location_mapping lm ON m.measurement_id = lm.measurement_id
                        WHERE m.is_current = 1 AND lm.measurement_id IS NULL
                    """

                cur.execute(query)

                mapping_records = []
                while True:
                    records = cur.fetchmany(BATCH_SIZE)
                    if not records:
                        break

                    batch_mappings = []
                    for record in records:
                        measurement_id = record[0]
                        try:
                            # Extract coordinates based on data type
                            points = self._extract_coordinates_from_record(record, data_type)

                            if not points:
                                ignored += 1
                                print(f"Registro {measurement_id} ignorado: no se pudieron extraer coordenadas")
                                continue

                            # IMPROVED DEBUG: Show coordinate ranges
                            if processed < 5:  # Only for first few records
                                coord_info = [(p.x, p.y) for p in points]
                                print(f"DEBUG - Record {measurement_id}: coordinates {coord_info}")

                            # IMPROVED MATCHING: Try individual points first, then all points
                            found_region = False

                            # First try: any point within any region
                            for region_id, region_geom in region_geometries.items():
                                for point in points:
                                    if point.within(region_geom):
                                        batch_mappings.append({
                                            'measurement_id': measurement_id,
                                            'region_id': region_id,
                                            'location_type': 'single_point'
                                        })
                                        found_region = True
                                        break
                                if found_region:
                                    break

                            # Second try: if all points are within the same region
                            if not found_region:
                                for region_id, region_geom in region_geometries.items():
                                    if all(point.within(region_geom) for point in points):
                                        batch_mappings.append({
                                            'measurement_id': measurement_id,
                                            'region_id': region_id,
                                            'location_type': 'all_points'
                                        })
                                        found_region = True
                                        break

                            if not found_region:
                                ignored += 1
                                # IMPROVED DEBUG: Show coordinate bounds for debugging
                                if ignored <= 10:  # Only for first 10 ignored records
                                    coord_bounds = [(p.x, p.y) for p in points]
                                    print(
                                        f"DEBUG - Registro {measurement_id} ignorado: puntos {coord_bounds} fuera de todas las regiones")

                        except Exception as e:
                            ignored += 1
                            print(f"Error procesando registro {measurement_id}: {str(e)}")

                    processed += len(records)
                    print(f"Procesados {processed} registros de {data_type}...")

                    if batch_mappings:
                        with conn.cursor() as insert_cur:
                            insert_cur.executemany("""
                                INSERT INTO location_mapping (measurement_id, region_id, location_type)
                                VALUES (%(measurement_id)s, %(region_id)s, %(location_type)s)
                                ON CONFLICT DO NOTHING
                            """, batch_mappings)
                        conn.commit()
                        mapping_records.extend(batch_mappings)

        # IMPROVED SUMMARY
        success_rate = ((len(mapping_records) / processed) * 100) if processed > 0 else 0
        print(f"\nTotal procesados ({data_type}): {processed}")
        print(f"Total ignorados ({data_type}): {ignored}")
        print(f"Total mappings creados ({data_type}): {len(mapping_records)}")
        print(f"Tasa de éxito mapeo ({data_type}): {success_rate:.1f}%")

        # DEBUG: Show sample coordinate ranges if mapping success is low
        if success_rate < 10 and processed > 0:
            print(f"DEBUG - Baja tasa de mapeo para {data_type}. Verificando rangos de coordenadas...")
            self._debug_coordinate_ranges(table_name, data_type)

        return len(mapping_records)

    def _debug_coordinate_ranges(self, table_name: str, data_type: str):
        """Debug function to show coordinate ranges"""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                if data_type == 'mobile':
                    cur.execute(f"""
                        SELECT 
                            MIN("StartLatitude") as min_start_lat,
                            MAX("StartLatitude") as max_start_lat,
                            MIN("StartLongitude") as min_start_lon,
                            MAX("StartLongitude") as max_start_lon,
                            MIN("EndLatitude") as min_end_lat,
                            MAX("EndLatitude") as max_end_lat,
                            MIN("EndLongitude") as min_end_lon,
                            MAX("EndLongitude") as max_end_lon
                        FROM {table_name} 
                        WHERE is_current = 1
                    """)
                    result = cur.fetchone()
                    print(f"DEBUG - Rangos de coordenadas móviles:")
                    print(f"  Start Lat: {result[0]} to {result[1]}")
                    print(f"  Start Lon: {result[2]} to {result[3]}")
                    print(f"  End Lat: {result[4]} to {result[5]}")
                    print(f"  End Lon: {result[6]} to {result[7]}")

                # Show sample of region geometries bounds
                cur.execute("""
                    SELECT region_id, geometry_data 
                    FROM geographic_regions 
                    WHERE is_current = 1 
                    LIMIT 3
                """)
                regions = cur.fetchall()
                print(f"DEBUG - Muestra de límites de regiones:")
                for r in regions:
                    try:
                        geom_data = r[1] if isinstance(r[1], dict) else json.loads(r[1])
                        geom = shape(geom_data)
                        bounds = geom.bounds  # (minx, miny, maxx, maxy)
                        print(f"  Region {r[0]}: bounds {bounds}")
                    except Exception as e:
                        print(f"  Region {r[0]}: error getting bounds - {e}")

    def _extract_coordinates_from_record(self, record, data_type: str):
        """Extract coordinate points directly from database record"""
        points = []

        try:
            if data_type == 'mobile':
                # Mobile data: record = (measurement_id, start_lat, start_lon, end_lat, end_lon)
                measurement_id, start_lat, start_lon, end_lat, end_lon = record

                # Check start location
                if start_lat is not None and start_lon is not None:
                    # IMPROVED VALIDATION: Check if coordinates are reasonable
                    if -90 <= start_lat <= 90 and -180 <= start_lon <= 180:
                        points.append(Point(start_lon, start_lat))

                # Check end location
                if end_lat is not None and end_lon is not None:
                    if -90 <= end_lat <= 90 and -180 <= end_lon <= 180:
                        points.append(Point(end_lon, end_lat))

            elif data_type == 'voice':
                # Voice data: record = (measurement_id, start_lat, start_lon, end_lat, end_lon, quality_lat, quality_lon)
                measurement_id, start_lat, start_lon, end_lat, end_lon, quality_lat, quality_lon = record

                # Check start location
                if start_lat is not None and start_lon is not None:
                    if -90 <= start_lat <= 90 and -180 <= start_lon <= 180:
                        points.append(Point(start_lon, start_lat))

                # Check end location
                if end_lat is not None and end_lon is not None:
                    if -90 <= end_lat <= 90 and -180 <= end_lon <= 180:
                        points.append(Point(end_lon, end_lat))

                # Check quality location (specific to voice data)
                if quality_lat is not None and quality_lon is not None:
                    if -90 <= quality_lat <= 90 and -180 <= quality_lon <= 180:
                        points.append(Point(quality_lon, quality_lat))

                # If no start/end but have quality location, use that
                if len(points) == 0 and quality_lat is not None and quality_lon is not None:
                    if -90 <= quality_lat <= 90 and -180 <= quality_lon <= 180:
                        points.append(Point(quality_lon, quality_lat))

        except Exception as e:
            print(f"Error extracting coordinates from record: {e}")

        return points

    def create_mapping_table(self):
        """Create location mapping table (works for both mobile and voice data)"""
        mapping_table = """
            CREATE TABLE IF NOT EXISTS location_mapping (
                measurement_id VARCHAR,
                region_id VARCHAR,
                location_type VARCHAR,
                PRIMARY KEY (measurement_id, region_id, location_type),
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

    def process_all_locations(self):
        """Process both mobile and voice locations in one call"""
        print("=== Procesando mapeo espacial para todos los tipos de datos ===")

        mobile_count = self.process_and_map_locations()
        voice_count = self.process_and_map_voice_locations()

        total_count = mobile_count + voice_count
        print(f"\n=== Resumen del mapeo espacial ===")
        print(f"Mappings móviles creados: {mobile_count}")
        print(f"Mappings de voz creados: {voice_count}")
        print(f"Total mappings creados: {total_count}")

        return total_count


def get_spatial_mapper(host: str, port: str, database: str, user: str, password: str):
    connection_params = {
        'host': host,
        'port': port,
        'database': database,
        'user': user,
        'password': password
    }
    return SpatialMapper(connection_params)
