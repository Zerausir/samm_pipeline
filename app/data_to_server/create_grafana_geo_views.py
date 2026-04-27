"""
create_grafana_geo_views.py  (versión definitiva)
──────────────────────────────────────────────────
Crea las vistas georeferenciadas que alimentan los 4 dashboards de Grafana.

Fuente de datos:
  mobile_raw_measurements  →  grafana_mobile_geo_view
  voice_raw_measurements   →  grafana_voice_geo_view

Por qué raw y no las vistas PowerBI (data_dashboard_view_visualization):
  Los dashboards de Grafana necesitan TODOS los datos regulatorios, incluyendo
  sesiones fallidas con coordenadas nulas (representan red que no respondió).

Filtros aplicados EN LA VISTA (no en la tabla):
  grafana_mobile_geo_view  →  "SessionType" IN ('HTTP Post', 'HTTP Download')
  grafana_voice_geo_view   →  "CallDirection" = 'MO'

  Esto permite crear vistas adicionales en el futuro (FTP, MT, etc.)
  sin reprocesar datos históricos.

Columnas añadidas sobre los datos raw:
  "Provincia"   → dpa_despro  (geographic_regions)
  "Cantón"      → dpa_descan  (geographic_regions)
  "Parroquia"   → dpa_despar  (geographic_regions)
  "PhoneNumber" → clave del filtro $PhoneNumber de Grafana (datasource_phones)
  "Device"      → modelo de dispositivo              (datasource_phones)
  "CZO"         → zona de control operativa          (datasource_phones)

Dependencias (deben existir antes de ejecutar):
  - mobile_raw_measurements   (main_raw_mobile.py)
  - voice_raw_measurements    (main_raw_voice.py)
  - location_mapping          (update_raw_mapping.py)
  - geographic_regions        (paso geográfico inicial)
  - datasource_phones         (load_datasource.py)

Queries Grafana compatibles (PostgreSQL):
  Ver comentario al pie del archivo.

Fix 2026-03-18:
  JOIN datasource_phones ahora usa mm."IMEI"::bigint y vm."IMEI"::bigint
  para resolver la incompatibilidad TEXT vs BIGINT entre las tablas raw
  y datasource_phones.
"""

import os
import psycopg2
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_env_var(var_name, default=None):
    value = os.environ.get(var_name)
    if value is None:
        load_dotenv()
        value = os.environ.get(var_name)
    return value if value is not None else default


def _get_connection_params() -> dict:
    return {
        "host": get_env_var("POSTGRES_HOST"),
        "port": get_env_var("POSTGRES_PORT"),
        "database": get_env_var("POSTGRES_DB"),
        "user": get_env_var("POSTGRES_USER"),
        "password": get_env_var("POSTGRES_PASSWORD"),
    }


# ---------------------------------------------------------------------------
# Columnas de negocio para cada vista
# Los filtros de SessionType y CallDirection se aplican en la definición
# de la vista, no en la tabla — máxima extensibilidad futura.
# ---------------------------------------------------------------------------

# Columnas requeridas para los dashboards de datos móviles HTTP
_MOBILE_COLS = [
    "DatasourceId",
    "SessionId",
    "SimOperator",
    "Operator",
    "IMEI",
    "IMSI",
    "StartTime",
    "EndTime",
    "SessionType",
    "EndServiceStatus",
    "ThroughputMbps",
    "StartLatitude",
    "StartLongitude",
    "EndLatitude",
    "EndLongitude",
    "StartRadioTechnology",
    "EndRadioTechnology",
]

# Columnas requeridas para los dashboards de calidad de voz
_VOICE_COLS = [
    "DatasourceId",
    "CallIndex",
    "SimOperator",
    "Operator",
    "IMEI",
    "IMSI",
    "StartTime",
    "EndTime",
    "CallDirection",
    "CallAttemptDateTime",
    "CallDroppedDateTime",
    "CallBlockedDateTime",
    "CallEstablishedDateTime",
    "DialEndServiceStatus",
    "AqmSessionEndAqmCallQuality",
    "AqmSessionEndAqmCallQualityDownlink",
    "AqmSessionEndAqmCallQualityUplink",
    "StartRadioTechnology",
    "EndRadioTechnology",
    "StartLatitude",
    "StartLongitude",
    "EndLatitude",
    "EndLongitude",
]


def _get_available_cols(cur, table_name: str, wanted: list):
    """Devuelve (disponibles, faltantes) comparando wanted con el schema real."""
    cur.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
        (table_name,),
    )
    existing = {row[0] for row in cur.fetchall()}
    available = [c for c in wanted if c in existing]
    missing = [c for c in wanted if c not in existing]
    if missing:
        print(f"  ⚠️  Columnas no encontradas en {table_name}: {missing}")
        print("      Se seleccionarán como NULL en la vista.")
    return available, missing


def _build_select(alias: str, available: list, missing: list) -> str:
    """
    Construye la cláusula SELECT:
      - Columnas disponibles → alias."ColName"
      - Columnas faltantes   → NULL::TEXT AS "ColName"
    """
    parts = [f'{alias}."{c}"' for c in available]
    parts += [f'NULL::TEXT AS "{c}"' for c in missing]
    return ",\n    ".join(parts)


# ---------------------------------------------------------------------------
# Verificación de dependencias
# ---------------------------------------------------------------------------

def _check_dependencies(cur) -> list:
    missing = []
    tables_needed = [
        "mobile_raw_measurements",
        "voice_raw_measurements",
        "geographic_regions",
        "location_mapping",
        "datasource_phones",
    ]
    for t in tables_needed:
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)",
            (t,),
        )
        if not cur.fetchone()[0]:
            missing.append(t)
    return missing


# ---------------------------------------------------------------------------
# Creación de vistas
# ---------------------------------------------------------------------------

def create_grafana_geo_views():
    """
    Crea (o recrea) grafana_mobile_geo_view y grafana_voice_geo_view.
    Levanta excepción si falla para que Airflow marque el task como FAILED.
    """
    print("=" * 65)
    print("Creando vistas georeferenciadas para Grafana (fuente: raw tables)...")
    print("=" * 65)

    with psycopg2.connect(**_get_connection_params()) as conn:
        with conn.cursor() as cur:

            # -- Verificar dependencias -------------------------------------
            print("\n🔍 Verificando dependencias...")
            missing_deps = _check_dependencies(cur)
            if missing_deps:
                raise RuntimeError(
                    "❌ Tablas faltantes (ejecutar pasos previos del DAG):\n"
                    + "\n".join(f"   - {d}" for d in missing_deps)
                )
            print("  ✅ Todas las dependencias presentes")

            # ================================================================
            # VISTA 1: grafana_mobile_geo_view
            # Fuente: mobile_raw_measurements (TODOS los datos, con nulos)
            # Filtro: SessionType IN ('HTTP Post', 'HTTP Download') en la vista
            # ================================================================
            print("\n📱 Creando grafana_mobile_geo_view...")
            cur.execute("DROP VIEW IF EXISTS grafana_mobile_geo_view;")

            avail_m, miss_m = _get_available_cols(cur, "mobile_raw_measurements", _MOBILE_COLS)
            select_mobile = _build_select("mm", avail_m, miss_m)

            cur.execute(f"""
CREATE VIEW grafana_mobile_geo_view AS
SELECT
    {select_mobile},
    gr.dpa_despro  AS "Provincia",
    gr.dpa_descan  AS "Cantón",
    gr.dpa_despar  AS "Parroquia",
    dp."PhoneNumber",
    dp."Device",
    dp."CZO"
FROM mobile_raw_measurements mm
LEFT JOIN location_mapping    lm ON mm.measurement_id = lm.measurement_id
LEFT JOIN geographic_regions  gr ON lm.region_id = gr.region_id
                                 AND gr.is_current = 1
LEFT JOIN datasource_phones   dp ON mm."IMEI"::bigint = dp."IMEI"
WHERE mm.is_current = 1
  AND mm."SessionType" IN ('HTTP Post', 'HTTP Download');
""")
            print("  ✅ grafana_mobile_geo_view creada")

            # Verificación rápida
            cur.execute("SELECT COUNT(*) FROM grafana_mobile_geo_view")
            total_m = cur.fetchone()[0]
            cur.execute(
                'SELECT COUNT(*) FROM grafana_mobile_geo_view WHERE "PhoneNumber" IS NOT NULL'
            )
            phone_m = cur.fetchone()[0]
            cur.execute(
                'SELECT COUNT(*) FROM grafana_mobile_geo_view WHERE "Provincia" IS NOT NULL'
            )
            geo_m = cur.fetchone()[0]
            print(f"     Total registros          : {total_m:,}")
            if total_m:
                print(f"     Con PhoneNumber resuelto : {phone_m:,} ({phone_m / total_m * 100:.1f}%)")
                print(f"     Con Provincia mapeada    : {geo_m:,} ({geo_m / total_m * 100:.1f}%)")

            # ================================================================
            # VISTA 2: grafana_voice_geo_view
            # Fuente: voice_raw_measurements (TODOS los datos, con nulos)
            # Filtro: CallDirection = 'MO' en la vista
            # ================================================================
            print("\n📞 Creando grafana_voice_geo_view...")
            cur.execute("DROP VIEW IF EXISTS grafana_voice_geo_view;")

            avail_v, miss_v = _get_available_cols(cur, "voice_raw_measurements", _VOICE_COLS)
            select_voice = _build_select("vm", avail_v, miss_v)

            cur.execute(f"""
CREATE VIEW grafana_voice_geo_view AS
SELECT
    {select_voice},
    gr.dpa_despro  AS "Provincia",
    gr.dpa_descan  AS "Cantón",
    gr.dpa_despar  AS "Parroquia",
    dp."PhoneNumber",
    dp."Device",
    dp."CZO"
FROM voice_raw_measurements   vm
LEFT JOIN location_mapping    lm ON vm.measurement_id = lm.measurement_id
LEFT JOIN geographic_regions  gr ON lm.region_id = gr.region_id
                                 AND gr.is_current = 1
LEFT JOIN datasource_phones   dp ON vm."IMEI"::bigint = dp."IMEI"
WHERE vm.is_current = 1
  AND vm."CallDirection" = 'MO';
""")
            print("  ✅ grafana_voice_geo_view creada")

            # Verificación rápida
            cur.execute("SELECT COUNT(*) FROM grafana_voice_geo_view")
            total_v = cur.fetchone()[0]
            cur.execute(
                'SELECT COUNT(*) FROM grafana_voice_geo_view WHERE "PhoneNumber" IS NOT NULL'
            )
            phone_v = cur.fetchone()[0]
            cur.execute(
                'SELECT COUNT(*) FROM grafana_voice_geo_view WHERE "Provincia" IS NOT NULL'
            )
            geo_v = cur.fetchone()[0]
            print(f"     Total registros          : {total_v:,}")
            if total_v:
                print(f"     Con PhoneNumber resuelto : {phone_v:,} ({phone_v / total_v * 100:.1f}%)")
                print(f"     Con Provincia mapeada    : {geo_v:,} ({geo_v / total_v * 100:.1f}%)")

        conn.commit()

    print("\n✅ Vistas Grafana creadas exitosamente")
    print("=" * 65)
    print("\nResumen:")
    print("  grafana_mobile_geo_view  →  HTTP Post + HTTP Download (regulatorio)")
    print("  grafana_voice_geo_view   →  Llamadas MO (regulatorio)")
    print("\nPara futuros análisis, crear vistas adicionales sobre las tablas raw:")
    print("  mobile_raw_measurements  →  cualquier SessionType")
    print("  voice_raw_measurements   →  cualquier CallDirection (MO/MT)")
    print("\nVariables Grafana compatibles:")
    print('  $SimOperator  → columna "SimOperator"')
    print('  $PhoneNumber  → columna "PhoneNumber"')
    print('  $__timeFrom() / $__timeTo() → "StartTime" / "EndTime"')


if __name__ == "__main__":
    create_grafana_geo_views()

# =============================================================================
# QUERIES GRAFANA — Adaptar en cada panel (PostgreSQL)
# =============================================================================
#
# DASHBOARD 1 — Métricas de Voz (fuente: grafana_voice_geo_view)
# ──────────────────────────────────────────────────────────────
# SELECT
#     SUM(CASE WHEN "CallAttemptDateTime"    IS NOT NULL THEN 1 ELSE 0 END) AS "Total Llamadas",
#     SUM(CASE WHEN "CallDroppedDateTime"    IS NOT NULL THEN 1 ELSE 0 END) AS "Llamadas Caídas",
#     SUM(CASE WHEN "CallBlockedDateTime"    IS NOT NULL THEN 1 ELSE 0 END) AS "Llamadas Bloqueadas",
#     SUM(CASE WHEN "CallEstablishedDateTime" IS NOT NULL
#              AND  "DialEndServiceStatus"   = 'Succeeded' THEN 1 ELSE 0 END) AS "Llamadas Establecidas",
#     SUM(CASE WHEN "DialEndServiceStatus"   = 'Failed'
#               OR  "DialEndServiceStatus"   IS NULL        THEN 1 ELSE 0 END) AS "Llamadas Fallidas"
# FROM grafana_voice_geo_view
# WHERE "StartTime" >= $__timeFrom()
#   AND "EndTime"   <= $__timeTo()
#   AND "SimOperator" IN (${SimOperator:sqlstring})
#   AND "PhoneNumber" IN (${PhoneNumber:sqlstring});
#
# DASHBOARD 2 — Sesiones HTTP (fuente: grafana_mobile_geo_view)
# ─────────────────────────────────────────────────────────────
# SELECT
#     CASE WHEN "SimOperator" = 'Claro'    THEN 'Conecel'
#          WHEN "SimOperator" = 'CNT'      THEN 'CNT EP'
#          WHEN "SimOperator" = 'Movistar' THEN 'Otecel'
#     END AS "Prestador",
#     COUNT(DISTINCT CONCAT("DatasourceId",':',CAST("SessionId" AS TEXT)))
#         FILTER (WHERE "SessionType" = 'HTTP Post')                         AS "Total UL",
#     COUNT(DISTINCT CONCAT("DatasourceId",':',CAST("SessionId" AS TEXT)))
#         FILTER (WHERE "SessionType" = 'HTTP Download')                     AS "Total DL",
#     COUNT(DISTINCT CONCAT("DatasourceId",':',CAST("SessionId" AS TEXT)))
#         FILTER (WHERE "SessionType" = 'HTTP Post'
#                   AND "EndServiceStatus" = 'Failed')                       AS "UL Fallidas",
#     COUNT(DISTINCT CONCAT("DatasourceId",':',CAST("SessionId" AS TEXT)))
#         FILTER (WHERE "SessionType" = 'HTTP Download'
#                   AND "EndServiceStatus" = 'Failed')                       AS "DL Fallidas"
# FROM grafana_mobile_geo_view
# WHERE "StartTime" >= $__timeFrom()
#   AND "EndTime"   <= $__timeTo()
#   AND "SimOperator" IN (${SimOperator:sqlstring})
#   AND "PhoneNumber" IN (${PhoneNumber:sqlstring})
# GROUP BY "SimOperator"
# ORDER BY CASE WHEN "SimOperator"='Claro' THEN 1
#               WHEN "SimOperator"='CNT'   THEN 2 ELSE 3 END;
#
# DASHBOARD 4 — Throughput HTTP (fuente: grafana_mobile_geo_view)
# ───────────────────────────────────────────────────────────────
# SELECT
#     CASE WHEN "SimOperator" = 'Claro'    THEN 'Conecel'
#          WHEN "SimOperator" = 'CNT'      THEN 'CNT EP'
#          WHEN "SimOperator" = 'Movistar' THEN 'Otecel'
#     END AS "Prestador",
#     CAST(AVG("ThroughputMbps")
#         FILTER (WHERE "SessionType"='HTTP Post'
#                   AND "EndServiceStatus"='Succeeded') AS DECIMAL(10,2)) AS "Promedio UL (Mbps)",
#     CAST(AVG("ThroughputMbps")
#         FILTER (WHERE "SessionType"='HTTP Download'
#                   AND "EndServiceStatus"='Succeeded') AS DECIMAL(10,2)) AS "Promedio DL (Mbps)"
# FROM grafana_mobile_geo_view
# WHERE "StartTime" >= $__timeFrom()
#   AND "EndTime"   <= $__timeTo()
#   AND "SimOperator" IN (${SimOperator:sqlstring})
#   AND "PhoneNumber" IN (${PhoneNumber:sqlstring})
# GROUP BY "SimOperator"
# ORDER BY CASE WHEN "SimOperator"='Claro' THEN 1
#               WHEN "SimOperator"='CNT'   THEN 2 ELSE 3 END;
#
# DASHBOARD 3 — Combinado Voz + HTTP (ver nota abajo)
# ─────────────────────────────────────────────────────
# En Grafana se implementa con dos paneles independientes sobre las dos vistas,
# o con una CTE que hace UNION entre los resultados. Ver implementación completa
# en la documentación del proyecto.
