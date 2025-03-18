import os
import psycopg2
from dotenv import load_dotenv


def create_dashboard_views():
    """
    Create views that join mobile_measurements, location_mapping and geographic_regions
    for easier visualization in Apache Superset, with improved column naming and performance.
    """
    load_dotenv()

    connection_params = {
        'host': os.getenv('POSTGRES_HOST'),
        'port': os.getenv('POSTGRES_PORT'),
        'database': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD')
    }

    try:
        with psycopg2.connect(**connection_params) as conn:
            with conn.cursor() as cur:
                # Check if views already exist
                cur.execute("SELECT EXISTS (SELECT FROM information_schema.views WHERE table_name = 'dashboard_view')")
                dashboard_view_exists = cur.fetchone()[0]

                cur.execute(
                    "SELECT EXISTS (SELECT FROM information_schema.views WHERE table_name = 'dashboard_view_visualization')")
                visualization_view_exists = cur.fetchone()[0]

                cur.execute(
                    "SELECT EXISTS (SELECT FROM information_schema.views WHERE table_name = 'geographic_view')")
                geographic_view_exists = cur.fetchone()[0]

                # Las vistas materializadas se almacenan en pg_matviews, no en information_schema.tables
                cur.execute(
                    "SELECT EXISTS (SELECT FROM pg_matviews WHERE matviewname = 'region_performance_summary')")
                materialized_view_exists = cur.fetchone()[0]

                # Eliminar las vistas en orden (de dependiente a base)
                # 1. Primero eliminar las vistas dependientes
                if visualization_view_exists:
                    print("Dropping existing visualization view...")
                    cur.execute("DROP VIEW IF EXISTS dashboard_view_visualization;")
                    print("Visualization view dropped successfully!")

                # 2. Luego eliminar las vistas base
                if dashboard_view_exists:
                    print("Dropping existing dashboard view...")
                    cur.execute("DROP VIEW IF EXISTS dashboard_view;")
                    print("Dashboard view dropped successfully!")

                if geographic_view_exists:
                    print("Dropping existing geographic view...")
                    cur.execute("DROP VIEW IF EXISTS geographic_view;")
                    print("Geographic view dropped successfully!")

                # 3. Finalmente eliminar la vista materializada
                if materialized_view_exists:
                    print("Dropping existing materialized view...")
                    # Utilizar IF EXISTS para evitar errores si ya fue eliminada
                    cur.execute("DROP MATERIALIZED VIEW IF EXISTS region_performance_summary;")
                    print("Materialized view dropped successfully!")

                # Crear las vistas en orden (de base a dependiente)
                # 1. Crear la vista dashboard base
                print("Creating dashboard view...")
                create_dashboard_view_sql = """
                CREATE VIEW dashboard_view AS
                SELECT 
                    m.measurement_id,
                    m.valid_from,
                    (m.measurement_data->>'start_time')::timestamp as start_time,
                    (m.measurement_data->>'end_time')::timestamp as end_time,
                    ((m.measurement_data->>'throughput_mbps')::float)::numeric(10,2) as throughput_mbps,
                    (m.measurement_data->>'end_file_size')::float as file_size,
                    (m.location_data->'start_location'->>'latitude')::float as start_lat,
                    (m.location_data->'start_location'->>'longitude')::float as start_lon,
                    (m.location_data->'end_location'->>'latitude')::float as end_lat,
                    (m.location_data->'end_location'->>'longitude')::float as end_lon,
                    m.radio_info->>'start_technology' as start_technology,
                    m.radio_info->>'end_technology' as end_technology,
                    m.operator_info->>'sim_operator' as "Operador",
                    m.operator_info->>'czo' as czo,
                    m.device_info->>'device_name' as device_name,
                    m.device_info->>'imei' as imei,
                    g.dpa_despro as "Provincia",
                    g.dpa_descan as "Cantón",
                    g.dpa_despar as "Parroquia"
                FROM mobile_measurements m
                JOIN location_mapping lm ON m.measurement_id = lm.measurement_id
                JOIN geographic_regions g ON lm.region_id = g.region_id
                WHERE m.is_current = 1 AND g.is_current = 1
                AND (m.measurement_data->>'throughput_mbps')::float IS NOT NULL;
                """
                cur.execute(create_dashboard_view_sql)
                print("Dashboard view created successfully!")

                # 2. Crear la vista de visualización
                print("Creating visualization view with throughput bins (no phantom records)...")
                create_visualization_view_sql = """
                CREATE VIEW dashboard_view_visualization AS
                SELECT 
                    measurement_id,
                    valid_from,
                    start_time,
                    end_time,
                    throughput_mbps as original_throughput_mbps,
                    LEAST(throughput_mbps, 70) as throughput_mbps,
                    CASE
                        WHEN throughput_mbps <= 7 THEN 7
                        WHEN throughput_mbps <= 14 THEN 14
                        WHEN throughput_mbps <= 21 THEN 21
                        WHEN throughput_mbps <= 28 THEN 28
                        WHEN throughput_mbps <= 35 THEN 35
                        WHEN throughput_mbps <= 42 THEN 42
                        WHEN throughput_mbps <= 49 THEN 49
                        WHEN throughput_mbps <= 56 THEN 56
                        WHEN throughput_mbps <= 63 THEN 63
                        WHEN throughput_mbps > 63 THEN 70
                    END as throughput_color_bin,
                    file_size,
                    start_lat,
                    start_lon,
                    end_lat,
                    end_lon,
                    start_technology,
                    end_technology,
                    "Operador",
                    czo,
                    device_name,
                    imei,
                    "Provincia",
                    "Cantón",
                    "Parroquia"
                FROM dashboard_view
                WHERE throughput_mbps IS NOT NULL;
                """
                cur.execute(create_visualization_view_sql)
                print("Visualization view created successfully without phantom records!")

                # 3. Crear la vista geográfica
                print("Creating geographic view for polygon visualization with corrected coordinate format...")
                create_geographic_view_sql = """
                CREATE VIEW geographic_view AS
                SELECT
                    gr.region_id,
                    gr.dpa_despro as "Provincia",
                    gr.dpa_descan as "Cantón",
                    gr.dpa_despar as "Parroquia",
                    -- Extraer y corregir el formato de las coordinates, eliminando un nivel de anidación
                    CASE 
                        WHEN gr.geometry_data->'coordinates'->0 IS NOT NULL THEN
                            -- Si hay al menos un elemento, tomamos el primer nivel de coordinates (para quitar un nivel de anidación)
                            gr.geometry_data->'coordinates'->0
                        ELSE 
                            -- Valor predeterminado si no hay datos
                            '[]'::jsonb
                    END as contour,
                    COUNT(m.measurement_id) as measurement_count,
                    COALESCE(AVG((m.measurement_data->>'throughput_mbps')::float), 0)::numeric(10,2) as avg_throughput,
                    COALESCE(MIN((m.measurement_data->>'throughput_mbps')::float), 0)::numeric(10,2) as min_throughput,
                    COALESCE(MAX((m.measurement_data->>'throughput_mbps')::float), 0)::numeric(10,2) as max_throughput
                FROM geographic_regions gr
                LEFT JOIN location_mapping lm ON gr.region_id = lm.region_id
                LEFT JOIN mobile_measurements m ON lm.measurement_id = m.measurement_id AND m.is_current = 1
                WHERE gr.is_current = 1
                GROUP BY gr.region_id, gr.dpa_despro, gr.dpa_descan, gr.dpa_despar, gr.geometry_data;
                """
                cur.execute(create_geographic_view_sql)
                print("Geographic view created successfully with corrected coordinate format for Superset!")

                # 4. Crear la vista materializada (usando IF NOT EXISTS para mayor seguridad)
                print("Creating materialized view...")
                create_materialized_view_sql = """
                CREATE MATERIALIZED VIEW IF NOT EXISTS region_performance_summary AS
                SELECT 
                    g.region_id,
                    g.dpa_despro as "Provincia",
                    g.dpa_descan as "Cantón",
                    g.dpa_despar as "Parroquia",
                    m.operator_info->>'sim_operator' as "Operador",
                    COUNT(m.measurement_id) as measurement_count,
                    (AVG((m.measurement_data->>'throughput_mbps')::float))::numeric(10,2) as avg_throughput,
                    (MIN((m.measurement_data->>'throughput_mbps')::float))::numeric(10,2) as min_throughput,
                    (MAX((m.measurement_data->>'throughput_mbps')::float))::numeric(10,2) as max_throughput
                FROM mobile_measurements m
                JOIN location_mapping lm ON m.measurement_id = lm.measurement_id
                JOIN geographic_regions g ON lm.region_id = g.region_id
                WHERE m.is_current = 1 AND g.is_current = 1
                AND (m.measurement_data->>'throughput_mbps')::float IS NOT NULL
                GROUP BY g.region_id, g.dpa_despro, g.dpa_descan, g.dpa_despar, m.operator_info->>'sim_operator';

                -- Create indexes on the materialized view for faster querying
                CREATE INDEX IF NOT EXISTS idx_region_performance_province 
                ON region_performance_summary("Provincia");

                CREATE INDEX IF NOT EXISTS idx_region_performance_canton 
                ON region_performance_summary("Cantón");

                CREATE INDEX IF NOT EXISTS idx_region_performance_parish 
                ON region_performance_summary("Parroquia");

                CREATE INDEX IF NOT EXISTS idx_region_performance_operator 
                ON region_performance_summary("Operador");
                """
                cur.execute(create_materialized_view_sql)
                print("Materialized view created successfully!")

                conn.commit()

                # Verify the views contain data
                try:
                    cur.execute("SELECT COUNT(*) FROM dashboard_view")
                    dashboard_count = cur.fetchone()[0]
                    print(f"Dashboard view contains {dashboard_count} records")
                except Exception as e:
                    print(f"Error counting dashboard_view records: {e}")

                try:
                    cur.execute("SELECT COUNT(*) FROM dashboard_view_visualization")
                    visualization_count = cur.fetchone()[0]
                    print(f"Visualization view contains {visualization_count} records (all real data)")
                except Exception as e:
                    print(f"Error counting dashboard_view_visualization records: {e}")

                try:
                    cur.execute("SELECT COUNT(*) FROM geographic_view")
                    geographic_count = cur.fetchone()[0]
                    print(f"Geographic view contains {geographic_count} records")
                except Exception as e:
                    print(f"Error counting geographic_view records: {e}")

                try:
                    cur.execute("SELECT COUNT(*) FROM region_performance_summary")
                    summary_count = cur.fetchone()[0]
                    print(f"Region performance summary contains {summary_count} records")
                except Exception as e:
                    print(f"Error counting region_performance_summary records: {e}")

    except Exception as e:
        print(f"Error: {e}")
        # Si es un error de conexión, proporcionar más detalles para facilitar la depuración
        if isinstance(e, psycopg2.OperationalError):
            print(
                "Error de conexión a la base de datos. Verifique sus credenciales y que la base de datos esté accesible.")


if __name__ == "__main__":
    create_dashboard_views()
