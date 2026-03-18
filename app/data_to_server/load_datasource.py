"""
load_datasource.py
──────────────────
Carga el catálogo de dispositivos (PhoneNumber ↔ IMSI ↔ IMEI) en la tabla
`datasource_phones` de PostgreSQL, enriquecido con Device y CZO provenientes
del archivo sense_nacional_v0.xlsx.

Flujo:
  1. Lee extract_datasource.parquet  → PhoneNumber, IMSI, IMEI
  2. Lee sense_nacional_v0.xlsx      → IMSI, IMEI, Device, CZO (+ otras cols)
  3. LEFT JOIN datasource ← sense    (clave: IMSI + IMEI)
  4. Upsert en datasource_phones     (ON CONFLICT DO NOTHING sobre phone_id)

La tabla resultante es usada en Fase 2 por las vistas grafana_*_geo_view
para resolver el filtro $PhoneNumber de Grafana sin modificar el schema
de mobile_measurements / voice_measurements.

Archivos requeridos en /opt/airflow/app/data/:
  - extract_datasource.parquet   (generado por samm_extract_data)
  - sense_nacional_v0.xlsx       (provisto manualmente)

Variables de entorno (mismo .env que el pipeline):
  POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
"""

import gc
import hashlib
import os

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch


# ---------------------------------------------------------------------------
# Helpers de entorno — mismo patrón que main.py y main_voice.py
# ---------------------------------------------------------------------------

def get_env_var(var_name, default=None):
    """Obtiene variable de entorno; si no existe, intenta cargar .env."""
    value = os.environ.get(var_name)
    if value is None:
        from dotenv import load_dotenv
        load_dotenv()
        value = os.environ.get(var_name)
    return value if value is not None else default


def _get_connection():
    """Devuelve una conexión psycopg2 con autocommit=False."""
    return psycopg2.connect(
        host=get_env_var("POSTGRES_HOST"),
        port=get_env_var("POSTGRES_PORT"),
        dbname=get_env_var("POSTGRES_DB"),
        user=get_env_var("POSTGRES_USER"),
        password=get_env_var("POSTGRES_PASSWORD"),
    )


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS datasource_phones (
    phone_id            VARCHAR PRIMARY KEY,
    "PhoneNumber"       VARCHAR NOT NULL,
    "IMSI"              BIGINT,
    "IMEI"              BIGINT,
    "Device"            VARCHAR,
    "CZO"               VARCHAR,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_INDEXES_SQL = [
    'CREATE INDEX IF NOT EXISTS idx_ds_imei   ON datasource_phones("IMEI");',
    'CREATE INDEX IF NOT EXISTS idx_ds_phone  ON datasource_phones("PhoneNumber");',
    'CREATE INDEX IF NOT EXISTS idx_ds_imsi   ON datasource_phones("IMSI");',
]

_UPSERT_SQL = """
INSERT INTO datasource_phones
    (phone_id, "PhoneNumber", "IMSI", "IMEI", "Device", "CZO")
VALUES
    (%(phone_id)s, %(PhoneNumber)s, %(IMSI)s, %(IMEI)s, %(Device)s, %(CZO)s)
ON CONFLICT (phone_id) DO NOTHING;
"""


# ---------------------------------------------------------------------------
# Carga y merge de datos
# ---------------------------------------------------------------------------

def _load_datasource_parquet(data_dir: str) -> pd.DataFrame:
    """Lee extract_datasource.parquet y normaliza tipos."""
    path = os.path.join(data_dir, "extract_datasource.parquet")
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"No se encontró extract_datasource.parquet en {data_dir}.\n"
            "Ejecuta primero extract_data_datasource.py en la máquina AD."
        )

    df = pd.read_parquet(path)
    print(f"  extract_datasource.parquet cargado: {len(df):,} filas")

    # Normalizar tipos
    df["PhoneNumber"] = df["PhoneNumber"].astype(str).str.strip()
    df["PhoneNumber"] = df["PhoneNumber"].replace({"None": None, "nan": None, "<NA>": None})
    for col in ("IMSI", "IMEI"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Eliminar filas sin PhoneNumber válido (no deberían existir por el WHERE del extractor)
    before = len(df)
    df = df[df["PhoneNumber"].notna() & (df["PhoneNumber"] != "")]
    if len(df) < before:
        print(f"  ⚠️  Descartadas {before - len(df):,} filas sin PhoneNumber válido")

    return df.drop_duplicates()


def _load_sense_nacional(data_dir: str) -> pd.DataFrame:
    """
    Lee sense_nacional_v0.xlsx y extrae IMSI, IMEI, Device, CZO.
    Misma ruta que usa main.py: /opt/airflow/app/data/sense_nacional_v0.xlsx
    """
    path = os.path.join(data_dir, "sense_nacional_v0.xlsx")
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"No se encontró sense_nacional_v0.xlsx en {data_dir}.\n"
            "Copia el archivo manualmente al servidor."
        )

    df = pd.read_excel(path)
    print(f"  sense_nacional_v0.xlsx cargado: {len(df):,} filas, columnas: {list(df.columns)}")

    # Convertir IMSI e IMEI al mismo tipo que datasource
    for col in ("IMSI", "IMEI"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Retener solo las columnas necesarias para el enriquecimiento
    cols_to_keep = ["IMSI", "IMEI"]
    if "Device" in df.columns:
        cols_to_keep.append("Device")
    else:
        print("  ⚠️  Columna 'Device' no encontrada en sense_nacional — se dejará NULL")
        df["Device"] = None
        cols_to_keep.append("Device")

    if "CZO" in df.columns:
        cols_to_keep.append("CZO")
    else:
        print("  ⚠️  Columna 'CZO' no encontrada en sense_nacional — se dejará NULL")
        df["CZO"] = None
        cols_to_keep.append("CZO")

    return df[cols_to_keep].drop_duplicates(subset=["IMSI", "IMEI"])


def _merge_with_sense(df_ds: pd.DataFrame, df_sense: pd.DataFrame) -> pd.DataFrame:
    """
    LEFT JOIN datasource ← sense en IMSI + IMEI.
    Si un dispositivo no está en sense_nacional, Device y CZO quedan NULL.
    """
    merged = df_ds.merge(df_sense, on=["IMSI", "IMEI"], how="left")

    matched = merged["Device"].notna().sum()
    print(f"  Match con sense_nacional: {matched:,}/{len(merged):,} "
          f"filas enriquecidas ({matched / len(merged) * 100:.1f}%)")

    return merged


def _generate_phone_id(row) -> str:
    """MD5 determinístico: PhoneNumber + IMEI (clave de negocio estable)."""
    key = f"{row['PhoneNumber']}|{row['IMEI'] if pd.notna(row['IMEI']) else ''}"
    return hashlib.md5(key.encode()).hexdigest()


def _prepare_records(df: pd.DataFrame) -> list:
    """Convierte el DataFrame a lista de dicts listos para execute_batch."""
    records = []
    for row in df.itertuples(index=False):
        phone_id = hashlib.md5(
            f"{row.PhoneNumber}|{row.IMEI if pd.notna(row.IMEI) else ''}".encode()
        ).hexdigest()

        records.append({
            "phone_id":    phone_id,
            "PhoneNumber": str(row.PhoneNumber) if pd.notna(row.PhoneNumber) else None,
            "IMSI":        int(row.IMSI) if pd.notna(row.IMSI) else None,
            "IMEI":        int(row.IMEI) if pd.notna(row.IMEI) else None,
            "Device":      str(row.Device) if pd.notna(row.Device) else None,
            "CZO":         str(row.CZO) if pd.notna(row.CZO) else None,
        })
    return records


# ---------------------------------------------------------------------------
# DDL en PostgreSQL
# ---------------------------------------------------------------------------

def _ensure_table(conn):
    """Crea la tabla datasource_phones e índices si no existen."""
    with conn.cursor() as cur:
        cur.execute(_CREATE_TABLE_SQL)
        for idx_sql in _INDEXES_SQL:
            cur.execute(idx_sql)
    conn.commit()
    print("  Tabla datasource_phones verificada/creada ✅")


# ---------------------------------------------------------------------------
# Función principal
# ---------------------------------------------------------------------------

def load_datasource():
    """
    Punto de entrada principal.
    Levanta excepción si algo falla (para que Airflow marque el task como FAILED).
    """
    data_dir = "/opt/airflow/app/data"

    print("=" * 60)
    print("Iniciando carga del catálogo de dispositivos...")
    print(f"  Directorio de datos: {data_dir}")
    print("=" * 60)

    # 1. Cargar fuentes
    print("\n📂 PASO 1: Cargando archivos...")
    df_ds    = _load_datasource_parquet(data_dir)
    df_sense = _load_sense_nacional(data_dir)

    # 2. Merge con sense_nacional para Device + CZO
    print("\n🔗 PASO 2: Merge con sense_nacional (IMSI + IMEI)...")
    df_merged = _merge_with_sense(df_ds, df_sense)
    del df_ds, df_sense
    gc.collect()

    # 3. Preparar registros
    print("\n⚙️  PASO 3: Preparando registros para PostgreSQL...")
    records = _prepare_records(df_merged)
    print(f"  Registros a insertar: {len(records):,}")
    del df_merged
    gc.collect()

    # 4. Conectar y hacer upsert
    print("\n🐘 PASO 4: Upsert en PostgreSQL...")
    conn = _get_connection()
    try:
        _ensure_table(conn)

        CHUNK_SIZE = 1000  # Tabla pequeña — chunk pequeño es suficiente
        total_inserted = 0

        with conn.cursor() as cur:
            for i in range(0, len(records), CHUNK_SIZE):
                chunk = records[i: i + CHUNK_SIZE]
                execute_batch(cur, _UPSERT_SQL, chunk, page_size=CHUNK_SIZE)
                total_inserted += len(chunk)
        conn.commit()

        print(f"  ✅ {total_inserted:,} registros procesados (ON CONFLICT DO NOTHING sobre duplicados)")

        # Verificación rápida
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM datasource_phones")
            final_count = cur.fetchone()[0]
        print(f"  Total registros en datasource_phones: {final_count:,}")

    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()

    print("\n✅ Carga del catálogo de dispositivos completada")
    print("=" * 60)


if __name__ == "__main__":
    load_datasource()
