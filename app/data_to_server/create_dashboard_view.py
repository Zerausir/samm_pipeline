import os
import psycopg2
from dotenv import load_dotenv


def get_env_var(var_name, default=None):
    """Get environment variable, optionally loading from .env file"""
    value = os.environ.get(var_name)
    if value is None:
        # Only load from .env if the variable isn't already set
        load_dotenv()
        value = os.environ.get(var_name)
    return value if value is not None else default


def create_dashboard_views():
    """
    Create views that join mobile_measurements, location_mapping and geographic_regions
    for easier visualization in Apache Superset, with improved column naming and performance.
    """
    # Use get_env_var instead of calling load_dotenv()
    connection_params = {
        'host': get_env_var('POSTGRES_HOST'),
        'port': get_env_var('POSTGRES_PORT'),
        'database': get_env_var('POSTGRES_DB'),
        'user': get_env_var('POSTGRES_USER'),
        'password': get_env_var('POSTGRES_PASSWORD')
    }

    try:
        # Connect to the database
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

                # Check for materialized views in pg_matviews
                cur.execute(
                    "SELECT EXISTS (SELECT FROM pg_matviews WHERE matviewname = 'region_performance_summary')")
                materialized_view_exists = cur.fetchone()[0]

                # Check for original views
                cur.execute(
                    "SELECT EXISTS (SELECT FROM information_schema.views WHERE table_name = 'vw_throughput_by_region')")
                throughput_view_exists = cur.fetchone()[0]

                cur.execute(
                    "SELECT EXISTS (SELECT FROM information_schema.views WHERE table_name = 'vw_technology_by_region')")
                technology_view_exists = cur.fetchone()[0]

                cur.execute(
                    "SELECT EXISTS (SELECT FROM information_schema.views WHERE table_name = 'vw_operator_performance')")
                operator_view_exists = cur.fetchone()[0]

                # Drop views in dependency order (dependent first, then base)
                # 1. First drop dependent views
                if visualization_view_exists:
                    print("Dropping existing visualization view...")
                    cur.execute("DROP VIEW IF EXISTS dashboard_view_visualization;")
                    print("Visualization view dropped successfully!")

                # 2. Then drop base views
                if dashboard_view_exists:
                    print("Dropping existing dashboard view...")
                    cur.execute("DROP VIEW IF EXISTS dashboard_view;")
                    print("Dashboard view dropped successfully!")

                if geographic_view_exists:
                    print("Dropping existing geographic view...")
                    cur.execute("DROP VIEW IF EXISTS geographic_view;")
                    print("Geographic view dropped successfully!")

                # 3. Drop original views
                if throughput_view_exists:
                    print("Dropping existing throughput view...")
                    cur.execute("DROP VIEW IF EXISTS vw_throughput_by_region;")
                    print("Throughput view dropped successfully!")

                if technology_view_exists:
                    print("Dropping existing technology view...")
                    cur.execute("DROP VIEW IF EXISTS vw_technology_by_region;")
                    print("Technology view dropped successfully!")

                if operator_view_exists:
                    print("Dropping existing operator view...")
                    cur.execute("DROP VIEW IF EXISTS vw_operator_performance;")
                    print("Operator view dropped successfully!")

                # 4. Finally drop materialized view
                if materialized_view_exists:
                    print("Dropping existing materialized view...")
                    cur.execute("DROP MATERIALIZED VIEW IF EXISTS region_performance_summary;")
                    print("Materialized view dropped successfully!")

                # Create views in order (base first, then dependent)

                # Original views from first script
                print("Creating throughput by region view...")
                throughput_view = """
                CREATE OR REPLACE VIEW vw_throughput_by_region AS
                SELECT 
                    gr.dpa_despro as province,
                    gr.dpa_descan as canton,
                    gr.dpa_despar as parish,
                    EXTRACT(YEAR FROM (mm.measurement_data->>'start_time')::timestamp) as year,
                    EXTRACT(MONTH FROM (mm.measurement_data->>'start_time')::timestamp) as month,
                    ROUND(AVG((mm.measurement_data->>'throughput_mbps')::numeric), 2) as avg_throughput,
                    COUNT(*) as measurement_count,
                    ROUND(MIN((mm.measurement_data->>'throughput_mbps')::numeric), 2) as min_throughput,
                    ROUND(MAX((mm.measurement_data->>'throughput_mbps')::numeric), 2) as max_throughput,
                    ROUND(STDDEV_SAMP((mm.measurement_data->>'throughput_mbps')::numeric), 2) as stddev_throughput
                FROM 
                    mobile_measurements mm
                JOIN 
                    location_mapping lm ON mm.measurement_id = lm.measurement_id
                JOIN 
                    geographic_regions gr ON lm.region_id = gr.region_id
                WHERE 
                    mm.is_current = 1 AND gr.is_current = 1
                GROUP BY 
                    gr.dpa_despro, gr.dpa_descan, gr.dpa_despar, 
                    EXTRACT(YEAR FROM (mm.measurement_data->>'start_time')::timestamp),
                    EXTRACT(MONTH FROM (mm.measurement_data->>'start_time')::timestamp)
                ORDER BY 
                    province, canton, parish, year, month;
                """
                cur.execute(throughput_view)
                print("Throughput by region view created successfully!")

                print("Creating technology by region view...")
                technology_view = """
                CREATE OR REPLACE VIEW vw_technology_by_region AS
                SELECT 
                    gr.dpa_despro as province,
                    gr.dpa_descan as canton,
                    mm.radio_info->>'start_technology' as technology,
                    COUNT(*) as measurement_count,
                    ROUND(AVG((mm.measurement_data->>'throughput_mbps')::numeric), 2) as avg_throughput
                FROM 
                    mobile_measurements mm
                JOIN 
                    location_mapping lm ON mm.measurement_id = lm.measurement_id
                JOIN 
                    geographic_regions gr ON lm.region_id = gr.region_id
                WHERE 
                    mm.is_current = 1 AND gr.is_current = 1
                GROUP BY 
                    gr.dpa_despro, gr.dpa_descan, mm.radio_info->>'start_technology'
                ORDER BY 
                    province, canton, technology;
                """
                cur.execute(technology_view)
                print("Technology by region view created successfully!")

                print("Creating operator performance view...")
                operator_view = """
                CREATE OR REPLACE VIEW vw_operator_performance AS
                SELECT 
                    mm.operator_info->>'sim_operator' as operator,
                    mm.radio_info->>'start_technology' as technology,
                    COUNT(*) as measurement_count,
                    ROUND(AVG((mm.measurement_data->>'throughput_mbps')::numeric), 2) as avg_throughput,
                    ROUND(MIN((mm.measurement_data->>'throughput_mbps')::numeric), 2) as min_throughput,
                    ROUND(MAX((mm.measurement_data->>'throughput_mbps')::numeric), 2) as max_throughput
                FROM 
                    mobile_measurements mm
                WHERE 
                    mm.is_current = 1 AND mm.operator_info->>'sim_operator' IS NOT NULL
                GROUP BY 
                    mm.operator_info->>'sim_operator', mm.radio_info->>'start_technology'
                ORDER BY 
                    operator, technology;
                """
                cur.execute(operator_view)
                print("Operator performance view created successfully!")

                # Views from second script
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

                print("Creating visualization view with throughput bins...")
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
                print("Visualization view created successfully!")

                print("Creating geographic view for polygon visualization...")
                create_geographic_view_sql = """
                CREATE VIEW geographic_view AS
                SELECT
                    gr.region_id,
                    gr.dpa_despro as "Provincia",
                    gr.dpa_descan as "Cantón",
                    gr.dpa_despar as "Parroquia",
                    -- Extract and correct coordinates format, removing one level of nesting
                    CASE 
                        WHEN gr.geometry_data->'coordinates'->0 IS NOT NULL THEN
                            -- If there's at least one element, take the first level of coordinates
                            gr.geometry_data->'coordinates'->0
                        ELSE 
                            -- Default value if no data
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
                print("Geographic view created successfully!")

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

                # Commit the changes
                conn.commit()

                # Verify the views contain data
                print("\nVerifying created views contain data:")

                try:
                    cur.execute("SELECT COUNT(*) FROM vw_throughput_by_region")
                    throughput_count = cur.fetchone()[0]
                    print(f"Throughput by region view contains {throughput_count} records")
                except Exception as e:
                    print(f"Error counting vw_throughput_by_region records: {e}")

                try:
                    cur.execute("SELECT COUNT(*) FROM vw_technology_by_region")
                    technology_count = cur.fetchone()[0]
                    print(f"Technology by region view contains {technology_count} records")
                except Exception as e:
                    print(f"Error counting vw_technology_by_region records: {e}")

                try:
                    cur.execute("SELECT COUNT(*) FROM vw_operator_performance")
                    operator_count = cur.fetchone()[0]
                    print(f"Operator performance view contains {operator_count} records")
                except Exception as e:
                    print(f"Error counting vw_operator_performance records: {e}")

                try:
                    cur.execute("SELECT COUNT(*) FROM dashboard_view")
                    dashboard_count = cur.fetchone()[0]
                    print(f"Dashboard view contains {dashboard_count} records")
                except Exception as e:
                    print(f"Error counting dashboard_view records: {e}")

                try:
                    cur.execute("SELECT COUNT(*) FROM dashboard_view_visualization")
                    visualization_count = cur.fetchone()[0]
                    print(f"Visualization view contains {visualization_count} records")
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

                print("\nAll dashboard views created successfully!")

    except Exception as e:
        print(f"Error creating dashboard views: {e}")
        # Provide more details for connection errors to help with debugging
        if isinstance(e, psycopg2.OperationalError):
            print("Database connection error. Verify your credentials and database accessibility.")


if __name__ == "__main__":
    create_dashboard_views()
