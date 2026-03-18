"""
update_raw_mapping.py — Actualización de mapeo espacial para datos raw.

NOTA: Este archivo reemplaza la versión legacy que usaba SpatialMapper(shapefile_path=...).
Las geometrías se cargan desde PostgreSQL (tabla geographic_regions), no desde shapefile.
"""
import os
from dotenv import load_dotenv
from app.utils.spatial_mapper import get_spatial_mapper


def get_env_var(var_name, default=None):
    """Get environment variable, optionally loading from .env file"""
    value = os.environ.get(var_name)
    if value is None:
        load_dotenv()
        value = os.environ.get(var_name)
    return value if value is not None else default


def update_raw_mappings() -> int:
    """
    Actualizar mapeo espacial para datos raw (móviles y voz).

    Reemplaza la implementación legacy que usaba shapefile_path.
    Ahora carga geometrías directamente desde PostgreSQL.
    """
    print("============================================================")
    print("Iniciando mapeo espacial de datos raw...")
    print(f"  Geometrías: tabla geographic_regions en PostgreSQL")
    print("============================================================")

    mapper = get_spatial_mapper(
        host=get_env_var('POSTGRES_HOST'),
        port=get_env_var('POSTGRES_PORT'),
        database=get_env_var('POSTGRES_DB'),
        user=get_env_var('POSTGRES_USER'),
        password=get_env_var('POSTGRES_PASSWORD'),
    )

    # Crear tabla de mapeo si no existe
    mapper.create_mapping_table()

    # Procesar ambos tipos de datos
    count = mapper.process_all_locations()
    print(f"Mapeo completado: {count} mappings creados")
    return count


if __name__ == "__main__":
    update_raw_mappings()
