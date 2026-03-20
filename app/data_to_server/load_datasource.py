"""
load_datasource.py
──────────────────
Carga el catálogo de dispositivos (PhoneNumber ↔ IMSI ↔ IMEI) en la tabla
`datasource_phones` de PostgreSQL, enriquecido con Device y CZO provenientes
del archivo sense_nacional_v0.xlsx.

Flujo:
  1. Lee extract_datasource.parquet  → PhoneNumber, IMSI, IMEI
  2. Lee sense_nacional_v0.xlsx      → IMSI, IMEI, Device, CZO
  3. LEFT JOIN datasource ← sense    (clave: IMSI + IMEI)
  4. Upsert en datasource_phones     (ON CONFLICT DO NOTHING sobre phone_id)
  5. Insertar registros de sense que no están en SQL Server
     (PhoneNumber extraído del campo Device: "OPE-0XXXXXXXXX" → "0XXXXXXXXX")
  6. Limpieza post-upsert:
       a. Eliminar registros sin Device/CZO
       b. Normalizar PhoneNumber: +593XXXXXXXXX → 0XXXXXXXXX
       c. Eliminar duplicados por IMEI causados por la normalización del +593
          (conservar el registro con PhoneNumber en formato 0XXXXXXXXX)

Archivos requeridos en /opt/airflow/app/data/:
  - extract_datasource.parquet   (generado por samm_extract_data)
  - sense_nacional_v0.xlsx       (provisto manualmente)

Variables de entorno:
  POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
"""

import gc
import hashlib
import os

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch


# ---------------------------------------------------------------------------
# Helpers de entorno
# ---------------------------------------------------------------------------

def get_env_var(var_name, default=None):
    value = os.environ.get(var_name)
    if value is None:
        from dotenv import load_dotenv
        load_dotenv()
        value = os.environ.get(var_name)
    return value if value is not None else default


def _get_connection():
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
    path = os.path.join(data_dir, "extract_datasource.parquet")
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"No se encontró extract_datasource.parquet en {data_dir}.\n"
            "Ejecuta primero extract_data_datasource.py en la máquina AD."
        )
    df = pd.read_parquet(path)
    print(f"  extract_datasource.parquet cargado: {len(df):,} filas")

    df["PhoneNumber"] = df["PhoneNumber"].astype(str).str.strip()
    df["PhoneNumber"] = df["PhoneNumber"].replace({"None": None, "nan": None, "<NA>": None})
    for col in ("IMSI", "IMEI"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    before = len(df)
    df = df[df["PhoneNumber"].notna() & (df["PhoneNumber"] != "")]
    if len(df) < before:
        print(f"  ⚠️  Descartadas {before - len(df):,} filas sin PhoneNumber válido")

    return df.drop_duplicates()


def _load_sense_nacional(data_dir: str) -> pd.DataFrame:
    path = os.path.join(data_dir, "sense_nacional_v0.xlsx")
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"No se encontró sense_nacional_v0.xlsx en {data_dir}.\n"
            "Copia el archivo manualmente al servidor."
        )
    df = pd.read_excel(path)
    print(f"  sense_nacional_v0.xlsx cargado: {len(df):,} filas")

    for col in ("IMSI", "IMEI"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    cols_to_keep = ["IMSI", "IMEI"]
    for col in ("Device", "CZO"):
        if col not in df.columns:
            print(f"  ⚠️  Columna '{col}' no encontrada en sense_nacional — se dejará NULL")
            df[col] = None
        cols_to_keep.append(col)

    return df[cols_to_keep].drop_duplicates(subset=["IMSI", "IMEI"])


def _merge_with_sense(df_ds: pd.DataFrame, df_sense: pd.DataFrame) -> pd.DataFrame:
    merged = df_ds.merge(df_sense, on=["IMSI", "IMEI"], how="left")
    matched = merged["Device"].notna().sum()
    print(f"  Match con sense_nacional: {matched:,}/{len(merged):,} "
          f"filas enriquecidas ({matched / len(merged) * 100:.1f}%)")
    return merged


def _prepare_records(df: pd.DataFrame) -> list:
    records = []
    for row in df.itertuples(index=False):
        phone_id = hashlib.md5(
            f"{row.PhoneNumber}|{row.IMEI if pd.notna(row.IMEI) else ''}".encode()
        ).hexdigest()
        records.append({
            "phone_id": phone_id,
            "PhoneNumber": str(row.PhoneNumber) if pd.notna(row.PhoneNumber) else None,
            "IMSI": int(row.IMSI) if pd.notna(row.IMSI) else None,
            "IMEI": int(row.IMEI) if pd.notna(row.IMEI) else None,
            "Device": str(row.Device) if pd.notna(row.Device) else None,
            "CZO": str(row.CZO) if pd.notna(row.CZO) else None,
        })
    return records


def _extract_phone_from_device(device: str) -> str:
    """
    Extrae el PhoneNumber del campo Device.
    Formato esperado: 'OPE-0XXXXXXXXX' → '0XXXXXXXXX'
    Ejemplo: 'CON-0986985968' → '0986985968'
             'BACKUP-CON-0982874769' → '0982874769'
    """
    # Tomar la última parte después del último guión
    parts = device.split("-")
    phone = parts[-1].strip()
    # Validar que sea un número de 10 dígitos empezando con 0
    if phone.isdigit() and len(phone) == 10 and phone.startswith("0"):
        return phone
    return None


# ---------------------------------------------------------------------------
# DDL en PostgreSQL
# ---------------------------------------------------------------------------

def _ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute(_CREATE_TABLE_SQL)
        for idx_sql in _INDEXES_SQL:
            cur.execute(idx_sql)
    conn.commit()
    print("  Tabla datasource_phones verificada/creada ✅")


# ---------------------------------------------------------------------------
# Insertar registros de sense que no están en SQL Server
# ---------------------------------------------------------------------------

def _insert_sense_only_records(conn, df_sense: pd.DataFrame):
    """
    Inserta registros de sense_nacional cuyos IMEIs no tienen entrada
    en dbo.Datasource (SQL Server). El PhoneNumber se extrae del campo Device.
    Estos son dispositivos válidos con mediciones pero sin registro en el
    catálogo de SQL Server.
    """
    with conn.cursor() as cur:
        # Obtener IMEIs ya presentes en datasource_phones
        cur.execute('SELECT "IMEI" FROM datasource_phones WHERE "IMEI" IS NOT NULL')
        existing_imeis = {row[0] for row in cur.fetchall()}

    # Filtrar registros de sense que no están en datasource_phones
    df_sense_copy = df_sense.copy()
    df_sense_copy['IMEI_int'] = df_sense_copy['IMEI'].apply(
        lambda x: int(x) if pd.notna(x) else None
    )
    missing = df_sense_copy[
        df_sense_copy['IMEI_int'].notna() &
        ~df_sense_copy['IMEI_int'].isin(existing_imeis)
        ]

    if missing.empty:
        print("  ✅ Todos los registros de sense_nacional ya están en datasource_phones")
        return 0

    print(f"  Insertando {len(missing)} registros de sense sin entrada en SQL Server...")
    records = []
    skipped = 0
    for row in missing.itertuples(index=False):
        phone = _extract_phone_from_device(str(row.Device))
        if not phone:
            print(f"  ⚠️  No se pudo extraer PhoneNumber de Device='{row.Device}' — omitido")
            skipped += 1
            continue

        phone_id = hashlib.md5(
            f"{phone}|{int(row.IMEI)}".encode()
        ).hexdigest()

        records.append({
            "phone_id": phone_id,
            "PhoneNumber": phone,
            "IMSI": int(row.IMSI) if pd.notna(row.IMSI) else None,
            "IMEI": int(row.IMEI),
            "Device": str(row.Device),
            "CZO": str(row.CZO) if pd.notna(row.CZO) else None,
        })

    if records:
        with conn.cursor() as cur:
            execute_batch(cur, _UPSERT_SQL, records, page_size=100)
        conn.commit()
        print(f"  ✅ {len(records)} registros de sense insertados")

    if skipped:
        print(f"  ⚠️  {skipped} registros omitidos por PhoneNumber no extraíble")

    return len(records)


# ---------------------------------------------------------------------------
# Limpieza post-upsert
# ---------------------------------------------------------------------------

def _clean_datasource_phones(conn):
    """
    Limpieza post-upsert en dos pasos:

    1. Eliminar registros sin Device/CZO — dispositivos no presentes en
       sense_nacional. No son útiles para los dashboards.

    2. Normalizar PhoneNumber: +593XXXXXXXXX → 0XXXXXXXXX y eliminar el
       duplicado generado por la normalización (conservar el registro cuyo
       PhoneNumber ya estaba en formato 0XXXXXXXXX, es decir el de menor
       phone_id tras normalizar).
    """
    with conn.cursor() as cur:
        # PASO 1: Eliminar registros sin Device/CZO
        cur.execute("""
            DELETE FROM datasource_phones
            WHERE "Device" IS NULL OR "CZO" IS NULL;
        """)
        deleted_null = cur.rowcount
        print(f"  🧹 Paso 1 — Eliminados {deleted_null} registros sin Device/CZO")

        # PASO 2a: Normalizar +593XXXXXXXXX → 0XXXXXXXXX
        cur.execute("""
            UPDATE datasource_phones
            SET "PhoneNumber" = '0' || SUBSTRING("PhoneNumber", 5)
            WHERE "PhoneNumber" LIKE '+593%';
        """)
        updated_phone = cur.rowcount
        print(f"  🧹 Paso 2 — Normalizados {updated_phone} PhoneNumbers (+593 → 0)")

        # PASO 2b: Eliminar duplicados por IMEI generados por la normalización
        # Solo elimina cuando hay más de un registro con el mismo IMEI,
        # conservando el que tiene phone_id más pequeño (determinístico).
        cur.execute("""
            DELETE FROM datasource_phones
            WHERE phone_id IN (
                SELECT phone_id
                FROM (
                    SELECT phone_id,
                           ROW_NUMBER() OVER (
                               PARTITION BY "IMEI"
                               ORDER BY phone_id
                           ) AS rn
                    FROM datasource_phones
                ) ranked
                WHERE rn > 1
            );
        """)
        deleted_dup = cur.rowcount
        print(f"  🧹 Paso 3 — Eliminados {deleted_dup} duplicados por IMEI")

    conn.commit()


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
    df_ds = _load_datasource_parquet(data_dir)
    df_sense = _load_sense_nacional(data_dir)

    # 2. Merge con sense_nacional para Device + CZO
    print("\n🔗 PASO 2: Merge con sense_nacional (IMSI + IMEI)...")
    df_merged = _merge_with_sense(df_ds, df_sense)
    del df_ds
    gc.collect()

    # 3. Preparar registros
    print("\n⚙️  PASO 3: Preparando registros para PostgreSQL...")
    records = _prepare_records(df_merged)
    print(f"  Registros a insertar: {len(records):,}")
    del df_merged
    gc.collect()

    # 4. Conectar, upsert
    print("\n🐘 PASO 4: Upsert en PostgreSQL...")
    conn = _get_connection()
    try:
        _ensure_table(conn)

        CHUNK_SIZE = 1000
        total_inserted = 0
        with conn.cursor() as cur:
            for i in range(0, len(records), CHUNK_SIZE):
                chunk = records[i: i + CHUNK_SIZE]
                execute_batch(cur, _UPSERT_SQL, chunk, page_size=CHUNK_SIZE)
                total_inserted += len(chunk)
        conn.commit()
        print(f"  ✅ {total_inserted:,} registros procesados (ON CONFLICT DO NOTHING)")

        # 5. Insertar registros de sense que no están en SQL Server
        print("\n📋 PASO 5: Insertando registros de sense sin entrada en SQL Server...")
        _insert_sense_only_records(conn, df_sense)

        # 6. Limpieza post-upsert
        print("\n🧹 PASO 6: Limpieza post-upsert...")
        _clean_datasource_phones(conn)

        # Verificación final
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM datasource_phones")
            final_count = cur.fetchone()[0]
            cur.execute('SELECT COUNT(*) FROM datasource_phones WHERE "Device" IS NULL')
            null_count = cur.fetchone()[0]

        print(f"\n  Total registros en datasource_phones : {final_count:,}")
        print(f"  Registros sin Device/CZO             : {null_count:,}")
        if null_count > 0:
            print("  ⚠️  Aún hay registros sin Device/CZO — verificar sense_nacional_v0.xlsx")

    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()

    print("\n✅ Carga del catálogo de dispositivos completada")
    print("=" * 60)


if __name__ == "__main__":
    load_datasource()
