"""
load_geographic_regions.py
──────────────────────────
Carga el shapefile de regiones geográficas del Ecuador en la tabla
geographic_regions de PostgreSQL.

La carga es idempotente:
  - Si geographic_regions ya tiene datos (is_current = 1) → se omite.
  - Si está vacía → carga el shapefile completo (1 081 parroquias).

Esta operación se ejecuta una sola vez en el ciclo de vida del sistema
o cuando se reinicia la base de datos desde cero.

Uso en DAG:
  Paso 1 — antes de process_raw_mobile_data.

Variables de entorno requeridas:
  POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
  SHAPEFILE_PATH  (default: /opt/airflow/app/data/states/shapefile.shp)
"""

import os

import geopandas as gpd
from dotenv import load_dotenv

from app.utils.postgres_handler import PostgresDataHandler


def get_env_var(var_name: str, default: str = None) -> str:
    """Lee variable de entorno, cargando .env si es necesario."""
    value = os.environ.get(var_name)
    if value is None:
        load_dotenv()
        value = os.environ.get(var_name)
    return value if value is not None else default


def load_geographic_regions() -> int:
    """
    Carga el shapefile en geographic_regions si la tabla está vacía.

    Returns:
        Número de regiones cargadas (0 si ya existían datos).
    """
    print("============================================================")
    print("Iniciando carga de regiones geográficas...")
    print("============================================================")

    shapefile_path = get_env_var(
        'SHAPEFILE_PATH',
        '/opt/airflow/app/data/states/shapefile.shp'
    )

    connection_params = {
        'host': get_env_var('POSTGRES_HOST'),
        'port': get_env_var('POSTGRES_PORT'),
        'database': get_env_var('POSTGRES_DB'),
        'user': get_env_var('POSTGRES_USER'),
        'password': get_env_var('POSTGRES_PASSWORD'),
    }

    handler = PostgresDataHandler(connection_params)
    print(f"Conexión a PostgreSQL establecida: "
          f"{connection_params['host']}:{connection_params['port']}")

    # Crear tabla si no existe (DDL idempotente)
    handler.create_tables()

    # Verificar si ya hay datos
    if not handler.should_insert_geographic_data():
        print("✅ geographic_regions ya contiene datos — se omite la carga.")
        print("============================================================")
        return 0

    # Verificar que el shapefile existe
    if not os.path.exists(shapefile_path):
        raise FileNotFoundError(
            f"Shapefile no encontrado: {shapefile_path}\n"
            "Asegúrate de que app/data/states/shapefile.shp esté presente en el contenedor."
        )

    # Cargar shapefile
    print(f"Cargando shapefile: {shapefile_path}")
    gdf = gpd.read_file(shapefile_path)
    print(f"Shapefile cargado: {len(gdf):,} regiones")

    # Insertar en PostgreSQL (lógica en postgres_handler)
    handler.upsert_geographic_data(gdf)

    print(f"✅ {len(gdf):,} regiones geográficas cargadas exitosamente.")
    print("============================================================")
    return len(gdf)


def main():
    count = load_geographic_regions()
    return f"Carga de regiones geográficas completada: {count} regiones"


if __name__ == "__main__":
    main()
