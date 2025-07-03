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


def create_voice_dashboard_views():
    """
    Create voice_dashboard_view_visualization that shows all voice data
    with original column names preserved AND geographic information included.
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
                # Check if voice view already exists
                cur.execute(
                    "SELECT EXISTS (SELECT FROM information_schema.views WHERE table_name = 'voice_dashboard_view_visualization')")
                voice_view_exists = cur.fetchone()[0]

                # Drop view if it exists
                if voice_view_exists:
                    print("Dropping existing voice dashboard view...")
                    cur.execute("DROP VIEW IF EXISTS voice_dashboard_view_visualization;")
                    print("Voice dashboard view dropped successfully!")

                # Get all columns from voice_measurements table (excluding metadata)
                cur.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'voice_measurements' 
                    AND column_name NOT IN ('measurement_id', 'valid_from', 'valid_to', 'is_current', 'batch_id', 'ingestion_timestamp')
                    ORDER BY ordinal_position
                """)

                data_columns = [row[0] for row in cur.fetchall()]

                if not data_columns:
                    print("No data columns found in voice_measurements table. Make sure data has been loaded first.")
                    return

                print(f"Found {len(data_columns)} data columns in voice_measurements table")

                # Build the view query with all original columns PLUS geographic columns
                voice_columns = ['vm.measurement_id', 'vm.valid_from'] + [f'vm."{col}"' for col in data_columns]

                # Geographic columns to add
                geo_columns = [
                    'gr.dpa_despar',
                    'gr.dpa_canton',
                    'gr.dpa_descan',
                    'gr.dpa_provin',
                    'gr.dpa_despro'
                ]

                all_columns = voice_columns + geo_columns
                columns_sql = ',\n    '.join(all_columns)

                create_voice_view_sql = f"""
                CREATE VIEW voice_dashboard_view_visualization AS
                SELECT 
                    {columns_sql}
                FROM voice_measurements vm
                LEFT JOIN location_mapping lm ON vm.measurement_id = lm.measurement_id
                LEFT JOIN geographic_regions gr ON lm.region_id = gr.region_id AND gr.is_current = 1
                WHERE vm.is_current = 1;
                """

                cur.execute(create_voice_view_sql)
                print("Voice dashboard view with geographic data created successfully!")

                # Commit the changes
                conn.commit()

                # Verify the view contains data
                print("\nVerifying created voice view contains data:")
                try:
                    cur.execute("SELECT COUNT(*) FROM voice_dashboard_view_visualization")
                    count = cur.fetchone()[0]
                    print(f"voice_dashboard_view_visualization contains {count} records")

                    # Show sample of available columns including geographic ones
                    if count > 0:
                        cur.execute("""
                            SELECT column_name 
                            FROM information_schema.columns 
                            WHERE table_name = 'voice_dashboard_view_visualization' 
                            ORDER BY ordinal_position
                        """)
                        all_columns = [row[0] for row in cur.fetchall()]
                        print(f"Total columns available: {len(all_columns)}")
                        print(f"Sample columns: {', '.join(all_columns[:10])}...")

                        # Check if geographic columns are present
                        geo_cols_found = [col for col in all_columns if col.startswith('dpa_')]
                        if geo_cols_found:
                            print(f"Geographic columns found: {', '.join(geo_cols_found)}")
                        else:
                            print("⚠️ Warning: No geographic columns found in view")

                        # Test geographic data availability
                        cur.execute("""
                            SELECT COUNT(*) 
                            FROM voice_dashboard_view_visualization 
                            WHERE dpa_despro IS NOT NULL
                        """)
                        geo_count = cur.fetchone()[0]
                        print(f"Records with geographic data: {geo_count}/{count} ({(geo_count / count * 100):.1f}%)")

                except Exception as e:
                    print(f"Error verifying voice_dashboard_view_visualization: {e}")

                print("\nVoice dashboard view with geographic data created successfully!")
                print("The view includes all original voice data columns PLUS geographic information")

    except Exception as e:
        print(f"Error creating voice dashboard views: {e}")
        # Provide more details for connection errors to help with debugging
        if isinstance(e, psycopg2.OperationalError):
            print("Database connection error. Verify your credentials and database accessibility.")


if __name__ == "__main__":
    create_voice_dashboard_views()
