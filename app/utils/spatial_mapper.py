import psycopg2
import json
from shapely.geometry import Point, shape
from typing import Dict
import gc


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

        # IMPROVEMENT 1: Smaller batch size for memory efficiency
        BATCH_SIZE = 500  # Reduced from 1000
        processed = 0
        ignored = 0

        print("Obteniendo regiones...")
        region_geometries = self._load_region_geometries()

        if not region_geometries:
            print("ERROR: No se pudieron cargar geometrías de regiones")
            return 0

        print(f"Procesando mediciones de {data_type}...")
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                # Build query based on data type to get coordinates directly
                query = self._build_coordinate_query(table_name, data_type)
                cur.execute(query)

                mapping_records = []
                batch_count = 0

                while True:
                    records = cur.fetchmany(BATCH_SIZE)
                    if not records:
                        break

                    batch_count += 1
                    batch_mappings = []

                    for record in records:
                        measurement_id = record[0]
                        try:
                            # Extract coordinates based on data type
                            points = self._extract_coordinates_from_record(record, data_type)

                            if not points:
                                ignored += 1
                                continue

                            # IMPROVEMENT 2: More efficient region matching
                            region_match = self._find_matching_region(points, region_geometries)

                            if region_match:
                                batch_mappings.append({
                                    'measurement_id': measurement_id,
                                    'region_id': region_match['region_id'],
                                    'location_type': region_match['location_type']
                                })
                            else:
                                ignored += 1

                        except Exception as e:
                            ignored += 1
                            if ignored <= 5:  # Only log first 5 errors
                                print(f"Error procesando registro {measurement_id}: {str(e)}")

                    processed += len(records)

                    # IMPROVEMENT 3: Progress reporting every 10 batches
                    if batch_count % 10 == 0:
                        print(f"Procesados {processed} registros de {data_type} (batch {batch_count})...")

                    # IMPROVEMENT 4: Insert batches and clear memory
                    if batch_mappings:
                        self._insert_batch_mappings(conn, batch_mappings)
                        mapping_records.extend(batch_mappings)

                    # IMPROVEMENT 5: Memory cleanup
                    del batch_mappings, records
                    gc.collect()

        # Final summary
        success_rate = ((len(mapping_records) / processed) * 100) if processed > 0 else 0
        print(f"\nResumen de mapeo ({data_type}):")
        print(f"  Total procesados: {processed:,}")
        print(f"  Total ignorados: {ignored:,}")
        print(f"  Mappings creados: {len(mapping_records):,}")
        print(f"  Tasa de éxito: {success_rate:.1f}%")

        # DEBUG: Show coordinate ranges if mapping success is low
        if success_rate < 10 and processed > 0:
            print(f"⚠️  Baja tasa de mapeo para {data_type}. Ejecutando diagnóstico...")
            self._debug_coordinate_ranges(table_name, data_type)

        return len(mapping_records)

    def _load_region_geometries(self):
        """Load and cache region geometries with error handling"""
        region_geometries = {}

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT region_id, geometry_data 
                        FROM geographic_regions 
                        WHERE is_current = 1 AND geometry_data IS NOT NULL
                    """)
                    regions = cur.fetchall()

                    valid_regions = 0
                    for region_id, geometry_data in regions:
                        try:
                            if geometry_data:
                                # IMPROVEMENT 6: Better JSON handling
                                if isinstance(geometry_data, str):
                                    geom_data = json.loads(geometry_data)
                                else:
                                    geom_data = geometry_data

                                region_geom = shape(geom_data)

                                # IMPROVEMENT 7: Validate geometry
                                if region_geom.is_valid:
                                    region_geometries[region_id] = region_geom
                                    valid_regions += 1
                                else:
                                    print(f"⚠️  Geometría inválida para región {region_id}")

                        except Exception as e:
                            print(f"⚠️  Error cargando geometría para región {region_id}: {e}")

                    print(f"✅ Cargadas {valid_regions} geometrías válidas de {len(regions)} regiones totales")
                    return region_geometries

        except Exception as e:
            print(f"❌ Error cargando geometrías de regiones: {e}")
            return {}

    def _build_coordinate_query(self, table_name: str, data_type: str):
        """Build optimized query based on data type using REAL column names"""
        # ✅ CORREGIDO: Usar solo las columnas que realmente existen
        if data_type == 'voice':
            # Verificar qué columnas de coordenadas existen realmente en voice_measurements
            base_query = f"""
                SELECT m.measurement_id, 
                       m."StartLatitude", m."StartLongitude",
                       m."EndLatitude", m."EndLongitude"
                FROM {table_name} m
                LEFT JOIN location_mapping lm ON m.measurement_id = lm.measurement_id
                WHERE m.is_current = 1 
                    AND lm.measurement_id IS NULL
                    AND (m."StartLatitude" IS NOT NULL OR m."EndLatitude" IS NOT NULL)
            """
        else:
            # Para mobile data
            base_query = f"""
                SELECT m.measurement_id, 
                       m."StartLatitude", m."StartLongitude",
                       m."EndLatitude", m."EndLongitude"
                FROM {table_name} m
                LEFT JOIN location_mapping lm ON m.measurement_id = lm.measurement_id
                WHERE m.is_current = 1 
                    AND lm.measurement_id IS NULL
                    AND (m."StartLatitude" IS NOT NULL OR m."EndLatitude" IS NOT NULL)
            """

        return base_query

    def _find_matching_region(self, points, region_geometries):
        """Efficiently find matching region for points"""
        if not points:
            return None

        # IMPROVEMENT 8: Optimized matching strategy
        # Strategy 1: Single point match (fastest)
        for point in points:
            for region_id, region_geom in region_geometries.items():
                try:
                    if point.within(region_geom):
                        return {
                            'region_id': region_id,
                            'location_type': 'single_point'
                        }
                except Exception:
                    continue

        # Strategy 2: All points in same region (if multiple points)
        if len(points) > 1:
            for region_id, region_geom in region_geometries.items():
                try:
                    if all(point.within(region_geom) for point in points):
                        return {
                            'region_id': region_id,
                            'location_type': 'all_points'
                        }
                except Exception:
                    continue

        return None

    def _insert_batch_mappings(self, conn, batch_mappings):
        """Insert batch mappings with error handling"""
        if not batch_mappings:
            return

        try:
            with conn.cursor() as insert_cur:
                insert_cur.executemany("""
                    INSERT INTO location_mapping (measurement_id, region_id, location_type)
                    VALUES (%(measurement_id)s, %(region_id)s, %(location_type)s)
                    ON CONFLICT (measurement_id, region_id, location_type) DO NOTHING
                """, batch_mappings)
            conn.commit()
        except Exception as e:
            print(f"⚠️  Error insertando batch de mappings: {e}")
            conn.rollback()

    def _extract_coordinates_from_record(self, record, data_type: str):
        """Extract coordinate points directly from database record with improved validation"""
        points = []

        try:
            # ✅ CORREGIDO: Usar la estructura real de la consulta
            # Tanto mobile como voice ahora usan la misma estructura: 5 columnas
            measurement_id, start_lat, start_lon, end_lat, end_lon = record

            # IMPROVEMENT 9: Better coordinate validation
            coords_to_check = [
                (start_lat, start_lon, "start"),
                (end_lat, end_lon, "end")
            ]

            # Validate and create points
            for lat, lon, point_type in coords_to_check:
                if self._is_valid_coordinate(lat, lon):
                    points.append(Point(lon, lat))

        except Exception as e:
            print(f"Error extrayendo coordenadas: {e}")

        return points

    def _is_valid_coordinate(self, lat, lon):
        """Improved coordinate validation"""
        if lat is None or lon is None:
            return False

        try:
            lat_float = float(lat)
            lon_float = float(lon)

            # IMPROVEMENT 10: More specific validation for Ecuador
            # Ecuador coordinates are roughly: Lat: -5 to 2, Lon: -92 to -75
            if not (-6 <= lat_float <= 3):  # Slightly broader range
                return False
            if not (-93 <= lon_float <= -74):  # Slightly broader range
                return False

            return True

        except (ValueError, TypeError):
            return False

    def _debug_coordinate_ranges(self, table_name: str, data_type: str):
        """Enhanced debug function using real column names"""
        print(f"\n🔍 Diagnóstico de coordenadas para {data_type}:")

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                # ✅ CORREGIDO: Usar las columnas reales
                cur.execute(f"""
                    SELECT 
                        MIN("StartLatitude") as min_start_lat,
                        MAX("StartLatitude") as max_start_lat,
                        MIN("StartLongitude") as min_start_lon,
                        MAX("StartLongitude") as max_start_lon,
                        MIN("EndLatitude") as min_end_lat,
                        MAX("EndLatitude") as max_end_lat,
                        MIN("EndLongitude") as min_end_lon,
                        MAX("EndLongitude") as max_end_lon,
                        COUNT(*) as total_records,
                        COUNT("StartLatitude") as start_lat_count,
                        COUNT("EndLatitude") as end_lat_count
                    FROM {table_name} 
                    WHERE is_current = 1
                """)
                result = cur.fetchone()
                print(f"  📊 Estadísticas de coordenadas:")
                print(f"    Total registros: {result[8]:,}")
                print(f"    Start Lat válidas: {result[9]:,} (rango: {result[0]:.6f} a {result[1]:.6f})")
                print(f"    Start Lon válidas: {result[9]:,} (rango: {result[2]:.6f} a {result[3]:.6f})")
                print(f"    End Lat válidas: {result[10]:,} (rango: {result[4]:.6f} a {result[5]:.6f})")
                print(f"    End Lon válidas: {result[10]:,} (rango: {result[6]:.6f} a {result[7]:.6f})")

                # Sample some coordinates for manual inspection
                cur.execute(f"""
                    SELECT "StartLatitude", "StartLongitude", "EndLatitude", "EndLongitude"
                    FROM {table_name} 
                    WHERE is_current = 1 
                        AND "StartLatitude" IS NOT NULL 
                        AND "StartLongitude" IS NOT NULL
                    LIMIT 5
                """)

                samples = cur.fetchall()

                print(f"  📋 Muestra de coordenadas:")
                for i, sample in enumerate(samples, 1):
                    print(
                        f"    {i}. Start: ({sample[0]:.6f}, {sample[1]:.6f}), End: ({sample[2]:.6f}, {sample[3]:.6f})")

    # Keep existing methods unchanged
    def create_mapping_table(self):
        """Create location mapping table (works for both mobile and voice data)"""
        mapping_table = """
            CREATE TABLE IF NOT EXISTS location_mapping (
                measurement_id VARCHAR,
                region_id VARCHAR,
                location_type VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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
        print("=== 🗺️  Iniciando mapeo espacial para todos los tipos de datos ===")

        mobile_count = self.process_and_map_locations()
        voice_count = self.process_and_map_voice_locations()

        total_count = mobile_count + voice_count
        print(f"\n=== 📊 Resumen del mapeo espacial ===")
        print(f"Mappings móviles creados: {mobile_count:,}")
        print(f"Mappings de voz creados: {voice_count:,}")
        print(f"Total mappings creados: {total_count:,}")

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
