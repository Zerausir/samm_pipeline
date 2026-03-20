"""
update_raw_mapping.py — Actualización de mapeo espacial para datos raw.

NOTA: Este archivo reemplaza la versión legacy que usaba SpatialMapper(shapefile_path=...).
Las geometrías se cargan desde PostgreSQL (tabla geographic_regions), no desde shapefile.

FIX 2026-03-20:
  Antes llamaba mapper.process_all_locations() que internamente apunta a
  mobile_measurements y voice_measurements (tablas clean / PowerBI).
  Los measurement_id de sesiones fallidas, sin ThroughputMbps o sin
  coordenadas que sólo existen en mobile_raw_measurements /
  voice_raw_measurements nunca recibían entrada en location_mapping,
  por lo que grafana_mobile_geo_view y grafana_voice_geo_view devolvían
  NULL en Provincia / Cantón / Parroquia para esos registros.

  Ahora llama mapper.process_all_raw_locations() que apunta correctamente
  a mobile_raw_measurements y voice_raw_measurements.
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

    Mapea coordenadas de mobile_raw_measurements y voice_raw_measurements
    hacia geographic_regions e inserta en location_mapping.

    Los registros sin coordenadas (sesiones que no llegaron a capturar GPS)
    se omiten correctamente — representan fallos de red, no errores de datos.
    """
    print("============================================================")
    print("Iniciando mapeo espacial de datos raw...")
    print(f"  Tablas origen : mobile_raw_measurements, voice_raw_measurements")
    print(f"  Geometrías    : tabla geographic_regions en PostgreSQL")
    print(f"  Destino       : tabla location_mapping")
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

    # FIX: process_all_raw_locations() apunta a las tablas raw,
    # no a las tablas clean como hacía process_all_locations().
    count = mapper.process_all_raw_locations()
    print(f"Mapeo raw completado: {count} mappings creados")
    return count


if __name__ == "__main__":
    update_raw_mappings()
