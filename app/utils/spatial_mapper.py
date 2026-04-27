"""
spatial_mapper.py — Mapeador espacial optimizado.

Cambios respecto a la versión original:
  1. _find_matching_region:
       - Original: bucle O(n_regions) por cada punto → O(n_points × n_regions).
       - Optimizado: STRtree (R-tree de Shapely) → O(n_points × log(n_regions)).
       - Ganancia típica para Ecuador (~1 000 parroquias): 50x–200x.

  2. _load_region_geometries → _load_region_geometries_cached:
       - Original: se llamaba dos veces por ejecución (mobile + voice), recargando
         desde PostgreSQL cada vez.
       - Optimizado: resultado almacenado en self._geometry_cache. La segunda
         llamada (voice) reutiliza el cache en memoria.

  3. _insert_batch_mappings:
       - Original: executemany (1 round-trip por registro).
       - Optimizado: execute_batch con page_size configurable.

  4. BATCH_SIZE: 500 → 2 000 (más eficiente para cursores de servidor).

  5. [FIX 2026-03-20] _find_matching_region_strtree:
       - Original: point.within(geometry) → False para puntos en la frontera
         del polígono (relación topológica estricta).
       - Corregido: geometry.covers(point) → True incluso en la frontera.
       - Impacto: puntos GPS sobre límites de parroquia (vías, ríos) ahora
         se mapean correctamente. Afectaba principalmente a sesiones fallidas
         cuyas coordenadas se capturan en zonas de baja cobertura / frontera.

  6. [FIX 2026-03-20] Nuevos métodos para tablas raw:
       - process_and_map_raw_mobile_locations() → mobile_raw_measurements
       - process_and_map_raw_voice_locations()  → voice_raw_measurements
       - process_all_raw_locations()            → ambas tablas raw
       - El método process_all_locations() original sigue apuntando a las
         tablas clean (mobile_measurements / voice_measurements) sin cambios.
       - Antes de este fix, update_raw_mapping.py llamaba process_all_locations()
         que mapeaba las tablas clean en lugar de las raw, dejando todos los
         measurement_id exclusivos de las tablas raw sin entrada en
         location_mapping y causando que grafana_mobile_geo_view y
         grafana_voice_geo_view devolvieran NULL en Provincia/Cantón/Parroquia
         para sesiones fallidas y otros registros sin ThroughputMbps.

  Todo lo demás (validación de coordenadas, lógica de negocio, API pública)
  se preserva sin cambios de comportamiento.
"""

import gc
import json
from typing import Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_batch
from shapely.geometry import Point, shape
from shapely.strtree import STRtree


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class SpatialMapper:
    BATCH_SIZE: int = 2_000  # filas por fetchmany()
    INSERT_PAGE_SIZE: int = 2_000  # filas por execute_batch()

    def __init__(self, connection_params: Dict[str, str]):
        self.connection_params = connection_params
        # --- shared geometry cache (FIX #2) ---
        # Populated on first call to _load_region_geometries_cached().
        # Format: { region_id: Shapely geometry }
        self._geometry_cache: Optional[Dict[str, object]] = None
        # STRtree and parallel list of region_ids (rebuilt alongside cache)
        self._strtree: Optional[STRtree] = None
        self._strtree_ids: Optional[List[str]] = None  # parallel list to _strtree geometries

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def _get_connection(self):
        return psycopg2.connect(**self.connection_params)

    # ------------------------------------------------------------------
    # Public API — tablas CLEAN (PowerBI)
    # ------------------------------------------------------------------

    def process_and_map_locations(self) -> int:
        """Process and map mobile measurement locations (clean table)."""
        return self._process_locations("mobile_measurements", "mobile")

    def process_and_map_voice_locations(self) -> int:
        """Process and map voice measurement locations (clean table)."""
        return self._process_locations("voice_measurements", "voice")

    def process_all_locations(self) -> int:
        """
        Process both mobile and voice CLEAN tables in one call.
        Shared geometry cache avoids double DB load.
        Used by: previous version of update mapping.
        """
        print("=== 🗺️  Iniciando mapeo espacial para datos CLEAN ===")
        mobile_count = self.process_and_map_locations()
        voice_count = self.process_and_map_voice_locations()
        total = mobile_count + voice_count
        print("\n=== 📊 Resumen del mapeo espacial CLEAN ===")
        print(f"Mappings móviles creados: {mobile_count:,}")
        print(f"Mappings de voz creados:  {voice_count:,}")
        print(f"Total mappings creados:   {total:,}")
        return total

    # ------------------------------------------------------------------
    # Public API — tablas RAW (Grafana / regulatorio)
    # [FIX 2026-03-20] Métodos nuevos — antes process_all_locations() era
    # llamado desde update_raw_mapping.py, lo que mapeaba las tablas clean
    # en lugar de las raw y dejaba sin mapeo a todos los registros
    # exclusivos de mobile_raw_measurements / voice_raw_measurements.
    # ------------------------------------------------------------------

    def process_and_map_raw_mobile_locations(self) -> int:
        """Process and map raw mobile measurement locations."""
        return self._process_locations("mobile_raw_measurements", "mobile_raw")

    def process_and_map_raw_voice_locations(self) -> int:
        """Process and map raw voice measurement locations."""
        return self._process_locations("voice_raw_measurements", "voice_raw")

    def process_all_raw_locations(self) -> int:
        """
        Process both mobile and voice RAW tables in one call.
        Shared geometry cache avoids double DB load.
        Used by: update_raw_mapping.py (paso 7 del DAG).
        """
        print("=== 🗺️  Iniciando mapeo espacial para datos RAW ===")
        mobile_count = self.process_and_map_raw_mobile_locations()
        voice_count = self.process_and_map_raw_voice_locations()
        total = mobile_count + voice_count
        print("\n=== 📊 Resumen del mapeo espacial RAW ===")
        print(f"Mappings móviles raw creados: {mobile_count:,}")
        print(f"Mappings de voz raw creados:  {voice_count:,}")
        print(f"Total mappings raw creados:   {total:,}")
        return total

    # ------------------------------------------------------------------
    # Mapping table DDL
    # ------------------------------------------------------------------

    def create_mapping_table(self):
        """Create location_mapping table and indexes if they don't exist."""
        mapping_table = """
            CREATE TABLE IF NOT EXISTS location_mapping (
                measurement_id VARCHAR,
                region_id      VARCHAR,
                location_type  VARCHAR,
                created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (measurement_id, region_id, location_type),
                FOREIGN KEY (region_id) REFERENCES geographic_regions(region_id)
            );
            CREATE INDEX IF NOT EXISTS idx_location_mapping_measurement
                ON location_mapping(measurement_id);
            CREATE INDEX IF NOT EXISTS idx_location_mapping_region
                ON location_mapping(region_id);
            CREATE INDEX IF NOT EXISTS idx_location_mapping_type
                ON location_mapping(location_type);
        """
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(mapping_table)
            conn.commit()

    # ------------------------------------------------------------------
    # Core processing loop
    # ------------------------------------------------------------------

    def _process_locations(self, table_name: str, data_type: str) -> int:
        """Generic method to process locations for any measurement table."""

        # 1. Count pending records
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT COUNT(m.measurement_id)
                    FROM {table_name} m
                    LEFT JOIN location_mapping lm ON m.measurement_id = lm.measurement_id
                    WHERE m.is_current = 1 AND lm.measurement_id IS NULL
                    """
                )
                new_records = cur.fetchone()[0]

        if new_records == 0:
            print(f"No hay nuevos registros de {data_type} para procesar")
            return 0

        print(f"Encontrados {new_records:,} nuevos registros de {data_type} para procesar")

        # 2. Load (or reuse) cached region geometries + STRtree (FIX #2)
        region_geometries, tree, tree_ids = self._load_region_geometries_cached()

        if not region_geometries:
            print("ERROR: No se pudieron cargar geometrías de regiones")
            return 0

        # 3. Stream measurements and map them
        processed = 0
        ignored = 0
        mapping_records: List[dict] = []

        print(f"Procesando mediciones de {data_type} …")

        query = self._build_coordinate_query(table_name, data_type)

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)

                batch_count = 0
                while True:
                    records = cur.fetchmany(self.BATCH_SIZE)
                    if not records:
                        break

                    batch_count += 1
                    batch_mappings: List[dict] = []

                    for record in records:
                        measurement_id = record[0]
                        try:
                            points = self._extract_coordinates_from_record(record, data_type)
                            if not points:
                                ignored += 1
                                continue

                            # FIX #1: STRtree lookup + covers() instead of within()
                            region_match = self._find_matching_region_strtree(
                                points, region_geometries, tree, tree_ids
                            )

                            if region_match:
                                batch_mappings.append(
                                    {
                                        "measurement_id": measurement_id,
                                        "region_id": region_match["region_id"],
                                        "location_type": region_match["location_type"],
                                    }
                                )
                            else:
                                ignored += 1

                        except Exception as e:
                            ignored += 1
                            if ignored <= 5:
                                print(f"Error procesando registro {measurement_id}: {e}")

                    processed += len(records)

                    if batch_count % 10 == 0:
                        print(
                            f"Procesados {processed:,} registros de {data_type} "
                            f"(batch {batch_count}) …"
                        )

                    # FIX #3: execute_batch for inserts
                    if batch_mappings:
                        self._insert_batch_mappings(conn, batch_mappings)
                        mapping_records.extend(batch_mappings)

                    del batch_mappings, records
                    gc.collect()

        # 4. Summary
        success_rate = (len(mapping_records) / processed * 100) if processed > 0 else 0
        print(f"\nResumen de mapeo ({data_type}):")
        print(f"  Total procesados: {processed:,}")
        print(f"  Total ignorados:  {ignored:,}")
        print(f"  Mappings creados: {len(mapping_records):,}")
        print(f"  Tasa de éxito:    {success_rate:.1f}%")

        if success_rate < 10 and processed > 0:
            print(f"⚠️  Baja tasa de mapeo para {data_type}. Ejecutando diagnóstico …")
            self._debug_coordinate_ranges(table_name, data_type)

        return len(mapping_records)

    # ------------------------------------------------------------------
    # Geometry cache + STRtree (FIX #1 + #2)
    # ------------------------------------------------------------------

    def _load_region_geometries_cached(
            self,
    ) -> Tuple[Dict[str, object], STRtree, List[str]]:
        """
        Load region geometries from PostgreSQL and build an STRtree index.

        Result is cached in self._geometry_cache so that successive calls
        (mobile → voice, or clean → raw) do not hit the database again.
        """
        if self._geometry_cache is not None:
            print(
                f"✅ Reutilizando cache de geometrías ({len(self._geometry_cache):,} regiones)"
            )
            return self._geometry_cache, self._strtree, self._strtree_ids

        print("Cargando geometrías de regiones desde PostgreSQL …")
        region_geometries: Dict[str, object] = {}

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT region_id, geometry_data
                        FROM geographic_regions
                        WHERE is_current = 1 AND geometry_data IS NOT NULL
                        """
                    )
                    regions = cur.fetchall()

            valid = 0
            for region_id, geometry_data in regions:
                try:
                    if geometry_data:
                        geom_data = (
                            json.loads(geometry_data)
                            if isinstance(geometry_data, str)
                            else geometry_data
                        )
                        geom = shape(geom_data)
                        if geom.is_valid:
                            region_geometries[region_id] = geom
                            valid += 1
                        else:
                            print(f"⚠️  Geometría inválida para región {region_id}")
                except Exception as e:
                    print(f"⚠️  Error cargando geometría {region_id}: {e}")

            print(
                f"✅ Cargadas {valid:,} geometrías válidas de {len(regions):,} regiones totales"
            )

            # Build STRtree — parallel list of ids keeps order
            ids = list(region_geometries.keys())
            geoms = [region_geometries[rid] for rid in ids]
            tree = STRtree(geoms)

            # Store in instance cache
            self._geometry_cache = region_geometries
            self._strtree = tree
            self._strtree_ids = ids

            return region_geometries, tree, ids

        except Exception as e:
            print(f"❌ Error cargando geometrías de regiones: {e}")
            return {}, STRtree([]), []

    # ------------------------------------------------------------------
    # Region matching with STRtree (FIX #1 + FIX #5)
    # ------------------------------------------------------------------

    def _find_matching_region_strtree(
            self,
            points: List[Point],
            region_geometries: Dict[str, object],
            tree: STRtree,
            tree_ids: List[str],
    ) -> Optional[dict]:
        """
        Find the region that contains any of the given points.

        Uses STRtree.query() to obtain only candidate geometries whose
        bounding boxes intersect with the point, then does exact covers()
        check only on those candidates.

        FIX #5: usa geometry.covers(point) en lugar de point.within(geometry).
          - within() es estricto: punto en la frontera → False.
          - covers() incluye la frontera: punto en el borde → True.
          - Esto corrige el mapeo de puntos GPS sobre límites de parroquia
            (vías, ríos), que afectaba principalmente a sesiones fallidas
            cuyas coordenadas se capturan en zonas de baja cobertura.

        Complexity: O(log n_regions) vs O(n_regions) original.
        """
        # Strategy 1: first point that falls within (or on the boundary of) any region
        for point in points:
            candidate_indices = tree.query(point)
            for idx in candidate_indices:
                region_id = tree_ids[idx]
                try:
                    # FIX #5: covers() = within() ∪ boundary
                    if region_geometries[region_id].covers(point):
                        return {"region_id": region_id, "location_type": "single_point"}
                except Exception:
                    continue

        # Strategy 2: all points in the same region
        if len(points) > 1:
            first_candidates = set(tree.query(points[0]))
            for point in points[1:]:
                first_candidates &= set(tree.query(point))

            for idx in first_candidates:
                region_id = tree_ids[idx]
                try:
                    if all(region_geometries[region_id].covers(p) for p in points):
                        return {"region_id": region_id, "location_type": "all_points"}
                except Exception:
                    continue

        return None

    # Keep the original linear-scan method for reference / testing
    def _find_matching_region(
            self, points: List[Point], region_geometries: Dict[str, object]
    ) -> Optional[dict]:
        """Original O(n) linear scan — kept for backward-compat / unit tests."""
        for point in points:
            for region_id, region_geom in region_geometries.items():
                try:
                    if region_geom.covers(point):
                        return {"region_id": region_id, "location_type": "single_point"}
                except Exception:
                    continue

        if len(points) > 1:
            for region_id, region_geom in region_geometries.items():
                try:
                    if all(region_geom.covers(p) for p in points):
                        return {"region_id": region_id, "location_type": "all_points"}
                except Exception:
                    continue

        return None

    # ------------------------------------------------------------------
    # Batch insert (FIX #3)
    # ------------------------------------------------------------------

    def _insert_batch_mappings(self, conn, batch_mappings: List[dict]):
        """
        Insert a batch of location mappings.

        Original: executemany (1 round-trip per row).
        Optimizado: execute_batch (groups rows into fewer round-trips).
        """
        if not batch_mappings:
            return
        try:
            with conn.cursor() as cur:
                execute_batch(
                    cur,
                    """
                    INSERT INTO location_mapping (measurement_id, region_id, location_type)
                    VALUES (%(measurement_id)s, %(region_id)s, %(location_type)s)
                    ON CONFLICT (measurement_id, region_id, location_type) DO NOTHING
                    """,
                    batch_mappings,
                    page_size=self.INSERT_PAGE_SIZE,
                )
            conn.commit()
        except Exception as e:
            print(f"⚠️  Error insertando batch de mappings: {e}")
            conn.rollback()

    # ------------------------------------------------------------------
    # Query builder
    # ------------------------------------------------------------------

    def _build_coordinate_query(self, table_name: str, data_type: str) -> str:
        """Build coordinate-fetch query (same for all table variants)."""
        return f"""
            SELECT m.measurement_id,
                   m."StartLatitude",  m."StartLongitude",
                   m."EndLatitude",    m."EndLongitude"
            FROM {table_name} m
            LEFT JOIN location_mapping lm ON m.measurement_id = lm.measurement_id
            WHERE m.is_current = 1
              AND lm.measurement_id IS NULL
              AND (m."StartLatitude" IS NOT NULL OR m."EndLatitude" IS NOT NULL)
        """

    # ------------------------------------------------------------------
    # Coordinate extraction & validation
    # ------------------------------------------------------------------

    def _extract_coordinates_from_record(
            self, record, data_type: str
    ) -> List[Point]:
        """Extract valid Shapely Points from a DB record tuple."""
        points: List[Point] = []
        try:
            measurement_id, start_lat, start_lon, end_lat, end_lon = record
            for lat, lon in [(start_lat, start_lon), (end_lat, end_lon)]:
                if self._is_valid_coordinate(lat, lon):
                    points.append(Point(float(lon), float(lat)))
        except Exception as e:
            print(f"Error extrayendo coordenadas: {e}")
        return points

    def _is_valid_coordinate(self, lat, lon) -> bool:
        """Validate coordinates for Ecuador's bounding box."""
        if lat is None or lon is None:
            return False
        try:
            lat_f, lon_f = float(lat), float(lon)
            return (-6 <= lat_f <= 3) and (-93 <= lon_f <= -74)
        except (ValueError, TypeError):
            return False

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def _debug_coordinate_ranges(self, table_name: str, data_type: str):
        """Print coordinate statistics and samples for debugging."""
        print(f"\n🔍 Diagnóstico de coordenadas para {data_type}:")
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        MIN("StartLatitude")  AS min_start_lat,
                        MAX("StartLatitude")  AS max_start_lat,
                        MIN("StartLongitude") AS min_start_lon,
                        MAX("StartLongitude") AS max_start_lon,
                        MIN("EndLatitude")    AS min_end_lat,
                        MAX("EndLatitude")    AS max_end_lat,
                        MIN("EndLongitude")   AS min_end_lon,
                        MAX("EndLongitude")   AS max_end_lon,
                        COUNT(*)              AS total_records,
                        COUNT("StartLatitude")AS start_lat_count,
                        COUNT("EndLatitude")  AS end_lat_count
                    FROM {table_name}
                    WHERE is_current = 1
                    """
                )
                result = cur.fetchone()
                print(f"  📊 Estadísticas de coordenadas:")
                print(f"    Total registros:     {result[8]:,}")
                print(f"    Start Lat válidas:   {result[9]:,}  (rango: {result[0]} – {result[1]})")
                print(f"    Start Lon válidas:   {result[9]:,}  (rango: {result[2]} – {result[3]})")
                print(f"    End Lat válidas:     {result[10]:,} (rango: {result[4]} – {result[5]})")
                print(f"    End Lon válidas:     {result[10]:,} (rango: {result[6]} – {result[7]})")

                cur.execute(
                    f"""
                    SELECT "StartLatitude", "StartLongitude",
                           "EndLatitude",   "EndLongitude"
                    FROM {table_name}
                    WHERE is_current = 1
                      AND "StartLatitude"  IS NOT NULL
                      AND "StartLongitude" IS NOT NULL
                    LIMIT 5
                    """
                )
                samples = cur.fetchall()
                print(f"  📋 Muestra de coordenadas:")
                for i, s in enumerate(samples, 1):
                    print(
                        f"    {i}. Start: ({s[0]:.6f}, {s[1]:.6f})  "
                        f"End: ({s[2]:.6f}, {s[3]:.6f})"
                    )


# ---------------------------------------------------------------------------
# Factory function (API pública sin cambios)
# ---------------------------------------------------------------------------

def get_spatial_mapper(
        host: str, port: str, database: str, user: str, password: str
) -> SpatialMapper:
    connection_params = {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
    }
    return SpatialMapper(connection_params)
