"""
postgres_handler.py — PostgreSQL data handler optimizado.

Cambios respecto a la versión original:
  1. upsert_measurements / upsert_voice_measurements:
       - Eliminado el patrón SAVEPOINT-por-registro (4 round-trips × N registros).
       - Reemplazado por execute_batch + INSERT … ON CONFLICT DO NOTHING.
       - chunk_size por defecto aumentado a 5 000 (configurable).
  2. _ensure_*_columns_exist:
       - Eliminado el patrón "una conexión por columna nueva".
       - Todos los ALTER TABLE se emiten en una única transacción.
  3. _prepare_data_with_guarantee:
       - Reemplazado df.iterrows() por df.itertuples() (~5x más rápido).
  4. create_raw_tables / upsert_raw_measurements / upsert_raw_voice_measurements:
       - Nuevas tablas mobile_raw_measurements y voice_raw_measurements.
       - Almacenan datos regulatorios completos (sin dropna de coordenadas
         ni throughput) para los dashboards de Grafana.
       - Reutilizan el motor _upsert_dataframe() y _batch_add_columns() existentes.
  5. Resto de la lógica preservada sin cambios.
"""

import datetime
import gc
import hashlib
import json
from typing import Any, Dict, List

import geopandas as gpd
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class PostgresDataHandler:
    def __init__(self, connection_params: Dict[str, str]):
        """Initialize PostgreSQL connection parameters."""
        self.connection_params = connection_params
        self._test_connection()

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    def _test_connection(self):
        """Test database connection on initialization."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            print("✅ PostgreSQL connection validated successfully")
        except Exception as e:
            raise ConnectionError(f"❌ Failed to connect to PostgreSQL: {e}")

    def _get_connection(self):
        """Create and return a new database connection."""
        try:
            conn = psycopg2.connect(
                dbname=self.connection_params["database"],
                user=self.connection_params["user"],
                password=self.connection_params["password"],
                host=self.connection_params["host"],
                port=self.connection_params["port"],
            )
            conn.autocommit = False
            return conn
        except psycopg2.Error as e:
            raise ConnectionError(f"Failed to connect to PostgreSQL: {e}")

    # ------------------------------------------------------------------
    # DDL — create clean tables & indexes (PowerBI)
    # ------------------------------------------------------------------

    def create_tables(self):
        """Create clean tables and indexes if they don't exist."""
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
        mobile_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_mobile_batch_id ON mobile_measurements(batch_id);",
            "CREATE INDEX IF NOT EXISTS idx_mobile_valid_from ON mobile_measurements(valid_from);",
            "CREATE INDEX IF NOT EXISTS idx_mobile_is_current ON mobile_measurements(is_current);",
            "CREATE INDEX IF NOT EXISTS idx_mobile_ingestion ON mobile_measurements(ingestion_timestamp);",
        ]
        voice_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_voice_batch_id ON voice_measurements(batch_id);",
            "CREATE INDEX IF NOT EXISTS idx_voice_valid_from ON voice_measurements(valid_from);",
            "CREATE INDEX IF NOT EXISTS idx_voice_is_current ON voice_measurements(is_current);",
            "CREATE INDEX IF NOT EXISTS idx_voice_ingestion ON voice_measurements(ingestion_timestamp);",
        ]
        geographic_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_geo_batch_id ON geographic_regions(batch_id);",
            "CREATE INDEX IF NOT EXISTS idx_geo_valid_from ON geographic_regions(valid_from);",
            "CREATE INDEX IF NOT EXISTS idx_geo_is_current ON geographic_regions(is_current);",
            "CREATE INDEX IF NOT EXISTS idx_geo_despro ON geographic_regions(dpa_despro);",
            "CREATE INDEX IF NOT EXISTS idx_geo_descan ON geographic_regions(dpa_descan);",
        ]

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(mobile_measurements_table)
                    cur.execute(geographic_table)
                    cur.execute(voice_measurements_table)
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

    # ------------------------------------------------------------------
    # DDL — create raw tables & indexes (Grafana / regulatorio)
    # ------------------------------------------------------------------

    def create_raw_tables(self):
        """
        Crea mobile_raw_measurements y voice_raw_measurements si no existen.

        Estas tablas almacenan datos regulatorios COMPLETOS:
          - Sin dropna de coordenadas (sesión fallida = dato válido)
          - Sin dropna de ThroughputMbps (NULL = red no respondió)
          - Sin filtro de SessionType ni CallDirection
            → Los dashboards de Grafana filtran en las vistas, no aquí.
        """
        mobile_raw_table = """
            CREATE TABLE IF NOT EXISTS mobile_raw_measurements (
                measurement_id VARCHAR PRIMARY KEY,
                valid_from TIMESTAMP NOT NULL,
                valid_to TIMESTAMP,
                is_current INTEGER DEFAULT 1,
                batch_id VARCHAR,
                ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
        voice_raw_table = """
            CREATE TABLE IF NOT EXISTS voice_raw_measurements (
                measurement_id VARCHAR PRIMARY KEY,
                valid_from TIMESTAMP NOT NULL,
                valid_to TIMESTAMP,
                is_current INTEGER DEFAULT 1,
                batch_id VARCHAR,
                ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
        raw_mobile_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_raw_mobile_batch_id    ON mobile_raw_measurements(batch_id);",
            "CREATE INDEX IF NOT EXISTS idx_raw_mobile_valid_from   ON mobile_raw_measurements(valid_from);",
            "CREATE INDEX IF NOT EXISTS idx_raw_mobile_is_current   ON mobile_raw_measurements(is_current);",
            "CREATE INDEX IF NOT EXISTS idx_raw_mobile_ingestion    ON mobile_raw_measurements(ingestion_timestamp);",
        ]
        raw_voice_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_raw_voice_batch_id    ON voice_raw_measurements(batch_id);",
            "CREATE INDEX IF NOT EXISTS idx_raw_voice_valid_from   ON voice_raw_measurements(valid_from);",
            "CREATE INDEX IF NOT EXISTS idx_raw_voice_is_current   ON voice_raw_measurements(is_current);",
            "CREATE INDEX IF NOT EXISTS idx_raw_voice_ingestion    ON voice_raw_measurements(ingestion_timestamp);",
        ]
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(mobile_raw_table)
                    cur.execute(voice_raw_table)
                    for idx in raw_mobile_indexes + raw_voice_indexes:
                        try:
                            cur.execute(idx)
                        except psycopg2.Error as e:
                            print(f"Warning: Could not create raw index: {e}")
                conn.commit()
                print("✅ Raw tables and indexes created/verified successfully")
        except Exception as e:
            print(f"❌ Error creating raw tables: {e}")
            raise

    # ------------------------------------------------------------------
    # Schema helpers — batch DDL
    # ------------------------------------------------------------------

    def _ensure_mobile_columns_exist(self, df: pd.DataFrame):
        """
        Add missing columns to mobile_measurements in a SINGLE transaction.

        Original: abría una conexión por columna nueva (N conexiones).
        Optimizado: un solo bloque ALTER TABLE por columna, una transacción.
        """
        critical_columns = [
            "DatasourceId", "SessionId",
            "StartLatitude", "StartLongitude",
            "EndLatitude", "EndLongitude",
        ]
        self._batch_add_columns("mobile_measurements", df, critical_columns)

    def _ensure_voice_columns_exist(self, df: pd.DataFrame):
        """Add missing columns to voice_measurements in a SINGLE transaction."""
        critical_columns = [
            "DatasourceId", "CallIndex", "IMSI", "IMEI",
            "EndLatitude", "EndLongitude",
        ]
        self._batch_add_columns("voice_measurements", df, critical_columns)

    def _ensure_raw_mobile_columns_exist(self, df: pd.DataFrame):
        """Add missing columns to mobile_raw_measurements in a SINGLE transaction."""
        critical_columns = [
            "DatasourceId", "SessionId",
            "StartLatitude", "StartLongitude",
            "EndLatitude", "EndLongitude",
        ]
        self._batch_add_columns("mobile_raw_measurements", df, critical_columns)

    def _ensure_raw_voice_columns_exist(self, df: pd.DataFrame):
        """Add missing columns to voice_raw_measurements in a SINGLE transaction."""
        critical_columns = [
            "DatasourceId", "CallIndex", "IMSI", "IMEI",
            "EndLatitude", "EndLongitude",
        ]
        self._batch_add_columns("voice_raw_measurements", df, critical_columns)

    def _batch_add_columns(self, table_name: str, df: pd.DataFrame, critical_columns: List[str]):
        """
        Core helper: detect missing columns and add them all in one transaction.
        Compartido por las tablas clean y raw — el nombre de tabla se pasa como argumento.
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    # 1. Fetch current schema once
                    cur.execute(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = %s
                        """,
                        (table_name,),
                    )
                    existing_columns = {row[0] for row in cur.fetchall()}

                    # 2. Determine which columns are missing
                    columns_to_add = [col for col in df.columns if col not in existing_columns]

                    if not columns_to_add:
                        print(f"✅ {table_name}: no new columns to add ({len(existing_columns)} existing)")
                        return

                    print(f"Adding {len(columns_to_add)} new column(s) to {table_name} …")

                    failed_critical = []
                    for col in columns_to_add:
                        pg_type = self._get_postgres_type_safe(df[col].dtype, df[col])
                        alter_query = sql.SQL(
                            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}"
                        ).format(
                            sql.Identifier(table_name),
                            sql.Identifier(col),
                            sql.SQL(pg_type),
                        )
                        try:
                            cur.execute(alter_query)
                            print(f"  ✅ {col} ({pg_type})")
                        except Exception as e:
                            if col in critical_columns:
                                failed_critical.append(col)
                                print(f"  ❌ CRITICAL column {col}: {e}")
                            else:
                                print(f"  ⚠️  Non-critical column {col} skipped: {e}")

                    # 3. Commit the whole batch
                    conn.commit()

                    if failed_critical:
                        raise ValueError(
                            f"❌ CRITICAL: Could not add essential columns to {table_name}: {failed_critical}"
                        )

                    # 4. Verify critical columns now exist
                    cur.execute(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = %s
                        """,
                        (table_name,),
                    )
                    final_columns = {row[0] for row in cur.fetchall()}
                    missing_critical = [c for c in critical_columns if c not in final_columns]
                    if missing_critical:
                        raise ValueError(
                            f"❌ CRITICAL: Missing essential columns after ALTER: {missing_critical}"
                        )

                    print(f"✅ {table_name} schema validated — {len(final_columns)} columns ready")

        except Exception as e:
            print(f"❌ Error ensuring columns exist on {table_name}: {e}")
            raise

    # ------------------------------------------------------------------
    # Upsert — clean mobile measurements (PowerBI)
    # ------------------------------------------------------------------

    def upsert_measurements(self, df: pd.DataFrame, chunk_size: int = 5000):
        """
        Upsert mobile measurement data → mobile_measurements (analítico/PowerBI).

        Original: 4 round-trips por registro (SAVEPOINT + SELECT + INSERT + RELEASE).
        Optimizado: execute_batch con INSERT … ON CONFLICT DO NOTHING.
                    chunk_size por defecto: 5 000 (era 1 000).
        """
        try:
            self._ensure_mobile_columns_exist(df)
            self._upsert_dataframe(
                df,
                table_name="mobile_measurements",
                data_type="mobile",
                chunk_size=chunk_size,
            )
        except Exception as e:
            print(f"❌ Critical error in upsert_measurements: {e}")
            raise

    # ------------------------------------------------------------------
    # Upsert — clean voice measurements (PowerBI)
    # ------------------------------------------------------------------

    def upsert_voice_measurements(self, df: pd.DataFrame, chunk_size: int = 5000):
        """
        Upsert voice measurement data → voice_measurements (analítico/PowerBI).

        Original: 4 round-trips por registro (SAVEPOINT + SELECT + INSERT + RELEASE).
        Optimizado: execute_batch con INSERT … ON CONFLICT DO NOTHING.
        """
        try:
            self._ensure_voice_columns_exist(df)
            self._upsert_dataframe(
                df,
                table_name="voice_measurements",
                data_type="voice",
                chunk_size=chunk_size,
            )
        except Exception as e:
            print(f"❌ Critical error in upsert_voice_measurements: {e}")
            raise

    # ------------------------------------------------------------------
    # Upsert — raw mobile measurements (Grafana / regulatorio)
    # ------------------------------------------------------------------

    def upsert_raw_measurements(self, df: pd.DataFrame, chunk_size: int = 5000):
        """
        Upsert de datos móviles crudos → mobile_raw_measurements (regulatorio/Grafana).

        Misma mecánica que upsert_measurements() — motor _upsert_dataframe() compartido.
        La tabla destino conserva filas con coordenadas nulas y ThroughputMbps nulo:
        una sesión fallida es un dato regulatorio válido.
        """
        try:
            self._ensure_raw_mobile_columns_exist(df)
            self._upsert_dataframe(
                df,
                table_name="mobile_raw_measurements",
                data_type="mobile",
                chunk_size=chunk_size,
            )
        except Exception as e:
            print(f"❌ Critical error in upsert_raw_measurements: {e}")
            raise

    # ------------------------------------------------------------------
    # Upsert — raw voice measurements (Grafana / regulatorio)
    # ------------------------------------------------------------------

    def upsert_raw_voice_measurements(self, df: pd.DataFrame, chunk_size: int = 5000):
        """
        Upsert de datos de voz crudos → voice_raw_measurements (regulatorio/Grafana).

        Misma mecánica que upsert_voice_measurements() — motor _upsert_dataframe() compartido.
        La tabla destino conserva todas las direcciones de llamada (MO y MT) y filas
        con coordenadas nulas: el filtro de CallDirection se aplica en la vista Grafana.
        """
        try:
            self._ensure_raw_voice_columns_exist(df)
            self._upsert_dataframe(
                df,
                table_name="voice_raw_measurements",
                data_type="voice",
                chunk_size=chunk_size,
            )
        except Exception as e:
            print(f"❌ Critical error in upsert_raw_voice_measurements: {e}")
            raise

    # ------------------------------------------------------------------
    # Core upsert engine — shared by all tables (clean + raw)
    # ------------------------------------------------------------------

    def _upsert_dataframe(
            self,
            df: pd.DataFrame,
            table_name: str,
            data_type: str,
            chunk_size: int,
    ):
        """
        Generic bulk-upsert: prepares records and calls execute_batch per chunk.

        Complejidad original : O(4N) round-trips  → O(N/chunk_size) round-trips
        Usado por: upsert_measurements, upsert_voice_measurements,
                   upsert_raw_measurements, upsert_raw_voice_measurements.
        """
        total_records = len(df)
        total_new = 0
        total_skipped = 0

        metadata_columns = [
            "measurement_id", "valid_from", "valid_to",
            "is_current", "batch_id", "ingestion_timestamp",
        ]
        data_columns = list(df.columns)
        all_columns = metadata_columns + data_columns

        # Build INSERT once (column list is static per batch)
        col_identifiers = sql.SQL(", ").join(
            sql.Identifier(c) for c in all_columns
        )
        placeholders = sql.SQL(", ").join(sql.Placeholder() * len(all_columns))
        insert_query = sql.SQL(
            "INSERT INTO {table} ({cols}) VALUES ({vals}) ON CONFLICT (measurement_id) DO NOTHING"
        ).format(
            table=sql.Identifier(table_name),
            cols=col_identifiers,
            vals=placeholders,
        )
        insert_query_str = insert_query.as_string(
            self._get_connection()  # single disposable conn just for .as_string()
        )

        total_chunks = (total_records + chunk_size - 1) // chunk_size
        print(f"Processing {total_records:,} {data_type} records in {total_chunks} chunk(s) of {chunk_size:,}")

        for chunk_idx, chunk_start in enumerate(range(0, total_records, chunk_size), start=1):
            chunk_end = min(chunk_start + chunk_size, total_records)
            chunk_df = df.iloc[chunk_start:chunk_end]

            print(f"  Chunk {chunk_idx}/{total_chunks} → rows {chunk_start:,}–{chunk_end:,}")

            # Prepare list of value tuples
            records = self._prepare_data_with_guarantee(chunk_df, data_type)
            tuples = [
                tuple(rec.get(col) for col in all_columns)
                for rec in records
            ]

            try:
                with self._get_connection() as conn:
                    with conn.cursor() as cur:
                        execute_batch(cur, insert_query_str, tuples, page_size=chunk_size)
                        inserted = cur.rowcount if cur.rowcount >= 0 else len(tuples)
                        skipped = len(tuples) - inserted
                    conn.commit()

                total_new += inserted
                total_skipped += skipped
                print(f"    Inserted {inserted:,} | Skipped (duplicates) {skipped:,}")

            except Exception as e:
                print(f"❌ Error on chunk {chunk_idx}: {e}")
                raise

            finally:
                del chunk_df, records, tuples
                gc.collect()

        print(
            f"\n✅ {data_type} upsert complete — "
            f"Inserted {total_new:,} | Skipped {total_skipped:,} | Total {total_records:,}"
        )

    # ------------------------------------------------------------------
    # Geographic data
    # ------------------------------------------------------------------

    def should_insert_geographic_data(self) -> bool:
        """Check if geographic_regions table is empty."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM geographic_regions")
                    count = cur.fetchone()[0]
                    return count == 0
        except Exception as e:
            print(f"❌ Error checking geographic data: {e}")
            return False

    def upsert_geographic_data(self, gdf: "gpd.GeoDataFrame"):
        """Insert geographic data only if the table is empty."""
        if not self.should_insert_geographic_data():
            print("Geographic data already exists in database. Skipping insertion.")
            return

        try:
            records = self._prepare_geographic_data(gdf)

            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    execute_batch(
                        cur,
                        """
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
                        """,
                        records,
                        page_size=1000,
                    )
                conn.commit()

            print(f"✅ Inserted {len(records)} geographic records")

        except Exception as e:
            print(f"❌ Error inserting geographic data: {e}")
            raise

    # ------------------------------------------------------------------
    # Record preparation (FIX #3 — itertuples instead of iterrows)
    # ------------------------------------------------------------------

    def _prepare_measurement_data(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        return self._prepare_data_with_guarantee(df, "mobile")

    def _prepare_voice_data(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        return self._prepare_data_with_guarantee(df, "voice")

    def _prepare_data_with_guarantee(self, df: pd.DataFrame, data_type: str) -> List[Dict[str, Any]]:
        """
        Guarantee 1:1 record mapping or fail explicitly.

        Original: df.iterrows() — crea una Series por fila (lento).
        Optimizado: df.itertuples() — ~5x más rápido para DataFrames grandes.

        Usado tanto por las tablas clean como por las raw — el measurement_id
        hash se calcula sobre los mismos campos de negocio independientemente
        de la tabla destino.
        """
        if data_type == "mobile":
            id_fields = [
                "DatasourceId", "SessionId",
                "StartLatitude", "StartLongitude",
                "EndLatitude", "EndLongitude",
            ]
        elif data_type == "voice":
            id_fields = [
                "DatasourceId", "CallIndex", "IMSI", "IMEI",
                "EndLatitude", "EndLongitude", "SentenceIndex",
            ]
        else:
            raise ValueError(f"Unknown data type: {data_type}")

        initial_count = len(df)
        print(f"🔒 GUARANTEE MODE: Preparing {initial_count:,} {data_type} records …")

        batch_id = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        col_names = list(df.columns)
        records: List[Dict[str, Any]] = []
        failed_records: List[tuple] = []

        # itertuples returns namedtuples — index access is O(1), no Series overhead
        for row in df.itertuples(index=False, name="Row"):
            row_dict = dict(zip(col_names, row))
            try:
                measurement_id = self._generate_deterministic_id_from_dict(row_dict, id_fields)
                record = {
                    "measurement_id": measurement_id,
                    "valid_from": datetime.datetime.utcnow(),
                    "valid_to": None,
                    "is_current": 1,
                    "batch_id": batch_id,
                    "ingestion_timestamp": None,  # DB DEFAULT
                }
                for col in col_names:
                    record[col] = self._convert_value_for_postgres(row_dict[col])

                records.append(record)

            except Exception as e:
                failed_records.append((len(records), str(e)))

        final_count = len(records)
        if final_count != initial_count:
            msg = (
                f"\n❌ GUARANTEE FAILED for {data_type}:\n"
                f"  Input:   {initial_count:,}\n"
                f"  Output:  {final_count:,}\n"
                f"  Failed:  {len(failed_records)}\n"
                f"  First 5: {failed_records[:5]}"
            )
            print(msg)
            raise ValueError(f"GUARANTEE VIOLATION: {data_type} {final_count}/{initial_count}")

        print(f"✅ GUARANTEE SATISFIED: {final_count:,} {data_type} records prepared (100%)")
        return records

    def _generate_deterministic_id(self, row: pd.Series, id_fields: List[str]) -> str:
        """Generate deterministic MD5 ID from a pandas Series row."""
        id_components = []
        for field in id_fields:
            value = row.get(field, "")
            if pd.notna(value) and str(value).strip():
                id_components.append(str(value).strip())
        if not id_components:
            raise ValueError(f"No valid ID components found in fields: {id_fields}")
        return hashlib.md5("_".join(id_components).encode()).hexdigest()

    def _generate_deterministic_id_from_dict(self, row_dict: dict, id_fields: List[str]) -> str:
        """Generate deterministic MD5 ID from a plain dict (used with itertuples path)."""
        id_components = []
        for field in id_fields:
            value = row_dict.get(field, "")
            if value is not None and not (isinstance(value, float) and value != value):
                s = str(value).strip()
                if s:
                    id_components.append(s)
        if not id_components:
            raise ValueError(f"No valid ID components found in fields: {id_fields}")
        return hashlib.md5("_".join(id_components).encode()).hexdigest()

    def _convert_value_for_postgres(self, value):
        """Convert pandas/Python values to PostgreSQL-compatible types."""
        if value is None:
            return None
        # NaN / NaT check
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        if isinstance(value, (pd.Timestamp, datetime.datetime)):
            return value
        if isinstance(value, pd.Timedelta):
            return value.total_seconds() if pd.notna(value) else None
        if isinstance(value, (int, float)):
            return value
        if isinstance(value, str):
            return value
        return str(value)

    # ------------------------------------------------------------------
    # Geographic record preparation
    # ------------------------------------------------------------------

    def _prepare_geographic_data(self, gdf: "gpd.GeoDataFrame") -> List[Dict[str, Any]]:
        """Prepare geographic data with proper error handling."""
        records = []
        batch_id = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        for _, row in gdf.iterrows():
            try:
                id_fields = ["DPA_DESPRO", "DPA_DESCAN", "DPA_DESPAR"]
                region_id = self._generate_deterministic_id(row, id_fields)

                geometry_data = None
                if pd.notna(row["geometry"]):
                    try:
                        geometry_json = json.loads(
                            gpd.GeoSeries([row["geometry"]]).to_json()
                        )
                        geometry_data = geometry_json["features"][0]["geometry"]
                    except Exception as e:
                        print(f"Warning: Could not process geometry: {e}")

                records.append(
                    {
                        "region_id": region_id,
                        "valid_from": datetime.datetime.utcnow(),
                        "is_current": 1,
                        "dpa_despar": row["DPA_DESPAR"] if pd.notna(row["DPA_DESPAR"]) else None,
                        "dpa_canton": row["DPA_CANTON"] if pd.notna(row["DPA_CANTON"]) else None,
                        "dpa_descan": row["DPA_DESCAN"] if pd.notna(row["DPA_DESCAN"]) else None,
                        "dpa_provin": row["DPA_PROVIN"] if pd.notna(row["DPA_PROVIN"]) else None,
                        "dpa_despro": row["DPA_DESPRO"] if pd.notna(row["DPA_DESPRO"]) else None,
                        "dpa_anio": int(row["DPA_ANIO"]) if pd.notna(row["DPA_ANIO"]) else None,
                        "fcode": row["fcode"] if pd.notna(row["fcode"]) else None,
                        "geometry_data": json.dumps(geometry_data) if geometry_data else None,
                        "batch_id": batch_id,
                    }
                )
            except Exception as e:
                print(f"❌ Error processing geographic record: {e}")
                continue

        return records

    # ------------------------------------------------------------------
    # Type mapping helpers
    # ------------------------------------------------------------------

    def _get_postgres_type_safe(self, pandas_dtype, column_sample=None) -> str:
        """Convert pandas dtype to optimal PostgreSQL type."""
        dtype_str = str(pandas_dtype).lower()

        if "int8" in dtype_str or "int16" in dtype_str:
            return "SMALLINT"
        elif "int32" in dtype_str:
            return "INTEGER"
        elif "int64" in dtype_str or "int" in dtype_str:
            return "BIGINT"
        elif "float32" in dtype_str:
            return "REAL"
        elif "float64" in dtype_str or "float" in dtype_str:
            return "DOUBLE PRECISION"
        elif "datetime" in dtype_str:
            return "TIMESTAMP"
        elif "timedelta" in dtype_str:
            return "DOUBLE PRECISION"
        elif "bool" in dtype_str:
            return "BOOLEAN"
        elif "object" in dtype_str or "string" in dtype_str:
            # Siempre TEXT para strings — VARCHAR(N) causa truncación si llegan
            # valores más largos en batches posteriores, sin ninguna ganancia de
            # rendimiento en PostgreSQL (TEXT y VARCHAR tienen storage idéntico).
            return "TEXT"
        else:
            return "TEXT"

    def _get_postgres_type(self, pandas_dtype, column_sample=None) -> str:
        """Legacy alias."""
        return self._get_postgres_type_safe(pandas_dtype, column_sample)


# ---------------------------------------------------------------------------
# Factory function
# ---------------------------------------------------------------------------

def get_postgres_handler(
        host: str, port: str, database: str, user: str, password: str
) -> PostgresDataHandler:
    """Create a PostgresDataHandler instance with connection validation."""
    return PostgresDataHandler(
        {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password,
        }
    )
