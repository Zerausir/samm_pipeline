"""
update_raw_mapping.py
─────────────────────
Mapeo espacial para las tablas regulatorias crudas:
  mobile_raw_measurements  →  location_mapping
  voice_raw_measurements   →  location_mapping

Replica exactamente el comportamiento de update_mapping.py pero sobre
las tablas raw. Reutiliza el mismo SpatialMapper y la misma tabla
location_mapping — un registro que aparezca en ambas tablas (clean y raw)
con el mismo measurement_id solo genera un mapping geográfico.

Notas:
  - Filas sin coordenadas válidas son ignoradas automáticamente por
    _build_coordinate_query (WHERE StartLatitude IS NOT NULL OR EndLatitude IS NOT NULL).
    Esas filas se conservan en las tablas raw pero no tienen mapping geográfico,
    lo cual es correcto: una sesión fallida sin coordenadas no puede mapearse.
  - El filtro de SessionType y CallDirection NO se aplica aquí — eso ocurre
    en la vista grafana_*_geo_view.
"""

import os

from app.utils.spatial_mapper import SpatialMapper


def get_env_var(var_name, default=None):
    value = os.environ.get(var_name)
    if value is None:
        from dotenv import load_dotenv
        load_dotenv()
        value = os.environ.get(var_name)
    return value if value is not None else default


def update_raw_mappings() -> int:
    """
    Mapea coordenadas de mobile_raw_measurements y voice_raw_measurements
    a geographic_regions vía location_mapping.

    Devuelve el total de nuevos mappings creados.
    Levanta excepción si falla (para que Airflow marque el task como FAILED).
    """
    connection_params = {
        "host":     get_env_var("POSTGRES_HOST"),
        "port":     get_env_var("POSTGRES_PORT"),
        "database": get_env_var("POSTGRES_DB"),
        "user":     get_env_var("POSTGRES_USER"),
        "password": get_env_var("POSTGRES_PASSWORD"),
    }

    shapefile_path = os.path.join(
        get_env_var("geojson_route", "/app/data/states"), "shapefile.shp"
    )

    print("=" * 60)
    print("Iniciando mapeo espacial de datos raw...")
    print(f"  Shapefile: {shapefile_path}")
    print("=" * 60)

    mapper = SpatialMapper(
        connection_params=connection_params,
        shapefile_path=shapefile_path,
    )

    # Usar el motor genérico _process_locations directamente —
    # los métodos públicos de SpatialMapper están hardcodeados a las tablas
    # clean; para las raw llamamos al motor con los nombres de tabla correctos.
    print("\n📱 Mapeando mobile_raw_measurements...")
    mobile_count = mapper._process_locations("mobile_raw_measurements", "mobile")

    print("\n📞 Mapeando voice_raw_measurements...")
    voice_count = mapper._process_locations("voice_raw_measurements", "voice")

    total = mobile_count + voice_count
    print(f"\n✅ Mapeo raw completado:")
    print(f"   Mappings móviles raw  : {mobile_count:,}")
    print(f"   Mappings voz raw      : {voice_count:,}")
    print(f"   Total                 : {total:,}")
    print("=" * 60)

    return total


if __name__ == "__main__":
    update_raw_mappings()
