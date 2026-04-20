from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    'owner': 'ivan',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'superset_etl_pipeline',
        default_args=default_args,
        description='Pipeline secuencial completo para procesar datos móviles y de voz para visualización en PowerBI y Grafana',
        schedule='0 9,14 * * *',  # ← schedule_interval → schedule
        catchup=False,
        tags=['etl', 'powerbi', 'grafana', 'mobile', 'voice', 'sequential']
) as dag:
    # ========== PASO 1: PROCESAR DATOS MÓVILES ==========

    def run_main_script():
        """Procesar datos móviles (datos principales)"""
        from app.data_to_server.main import main
        main()
        return "Procesamiento de datos móviles completado"


    process_mobile_data = PythonOperator(
        task_id='process_mobile_data',
        python_callable=run_main_script,
        dag=dag,
        doc_md="""
        ### PASO 1: Procesar Datos Móviles

        Primera etapa del pipeline secuencial:
        - Carga datos desde archivos parquet móviles
        - Procesa y limpia los datos
        - Calcula throughput
        - Almacena en mobile_measurements
        - Inserta datos geográficos si es necesario
        """,
    )


    # ========== PASO 2: PROCESAR DATOS DE VOZ ==========

    def run_voice_script():
        """Procesar datos de voz"""
        from app.data_to_server.main_voice import main
        main()
        return "Procesamiento de datos de voz completado"


    process_voice_data = PythonOperator(
        task_id='process_voice_data',
        python_callable=run_voice_script,
        dag=dag,
        doc_md="""
        ### PASO 2: Procesar Datos de Voz

        Segunda etapa del pipeline secuencial:
        - Carga datos desde archivos parquet de voz
        - Procesa y merge múltiples tablas
        - Limpia y valida datos de llamadas
        - Almacena en voice_measurements preservando estructura original
        """,
    )


    # ========== PASO 3 [NUEVO]: PROCESAR DATOS MÓVILES RAW ==========

    def run_raw_mobile_script():
        """Procesar datos móviles crudos para uso regulatorio en Grafana"""
        from app.data_to_server.main_raw_mobile import main
        main()
        return "Procesamiento de datos móviles raw completado"


    process_raw_mobile_data = PythonOperator(
        task_id='process_raw_mobile_data',
        python_callable=run_raw_mobile_script,
        dag=dag,
        doc_md="""
        ### PASO 3: Procesar Datos Móviles Raw (regulatorio / Grafana)

        - Mismos parquets que el paso 1
        - Sin dropna — sesiones fallidas son datos regulatorios válidos
        - Sin filtro de SessionType — la vista Grafana filtra HTTP Post/Download
        - Almacena en mobile_raw_measurements
        """,
    )


    # ========== PASO 4 [NUEVO]: PROCESAR DATOS DE VOZ RAW ==========

    def run_raw_voice_script():
        """Procesar datos de voz crudos para uso regulatorio en Grafana"""
        from app.data_to_server.main_raw_voice import main
        main()
        return "Procesamiento de datos de voz raw completado"


    process_raw_voice_data = PythonOperator(
        task_id='process_raw_voice_data',
        python_callable=run_raw_voice_script,
        dag=dag,
        doc_md="""
        ### PASO 4: Procesar Datos de Voz Raw (regulatorio / Grafana)

        - Mismos parquets que el paso 2
        - Sin dropna — llamadas fallidas son datos regulatorios válidos
        - Sin filtro de CallDirection — la vista Grafana filtra CallDirection='MO'
        - Almacena en voice_raw_measurements
        """,
    )


    # ========== PASO 5 [NUEVO]: CARGAR CATÁLOGO DE DISPOSITIVOS ==========

    def run_load_datasource():
        """Cargar catálogo PhoneNumber/IMSI/IMEI enriquecido con Device y CZO"""
        from app.data_to_server.load_datasource import load_datasource
        load_datasource()
        return "Catálogo de dispositivos cargado en datasource_phones"


    load_datasource_task = PythonOperator(
        task_id='load_datasource',
        python_callable=run_load_datasource,
        dag=dag,
        doc_md="""
        ### PASO 5: Cargar Catálogo de Dispositivos

        - Lee extract_datasource.parquet (PhoneNumber, IMSI, IMEI)
        - Enriquece con Device y CZO desde sense_nacional_v0.xlsx
        - Upsert en datasource_phones (ON CONFLICT DO NOTHING)

        Prerequisito: extract_data_datasource.py ejecutado en máquina AD
        y el Parquet copiado a app/data/.
        """,
    )


    # ========== PASO 6: MAPEO ESPACIAL COMPLETO (datos clean) ==========

    def run_update_all_mappings():
        """Actualizar mapeo espacial para AMBOS tipos de datos"""
        from app.data_to_server.update_mapping import update_all_mappings
        count = update_all_mappings()
        return f"Mapeo espacial completado: {count} mappings totales creados"


    update_all_mappings = PythonOperator(
        task_id='update_all_mappings',
        python_callable=run_update_all_mappings,
        dag=dag,
        doc_md="""
        ### PASO 6: Actualizar Mapeo Espacial Completo

        - Procesa coordenadas de AMBOS tipos de datos (clean)
        - Mapea puntos a regiones geográficas
        - Actualiza tabla location_mapping para móviles Y voz
        - Usa métodos especializados para cada tipo
        """,
    )


    # ========== PASO 7 [NUEVO]: MAPEO ESPACIAL RAW ==========

    def run_update_raw_mappings():
        """Actualizar mapeo espacial para datos raw (regulatorio)"""
        from app.data_to_server.update_raw_mapping import update_raw_mappings
        count = update_raw_mappings()
        return f"Mapeo espacial raw completado: {count} mappings totales creados"


    update_raw_mappings_task = PythonOperator(
        task_id='update_raw_mappings',
        python_callable=run_update_raw_mappings,
        dag=dag,
        doc_md="""
        ### PASO 7: Mapeo Espacial — Datos Raw

        - mobile_raw_measurements + voice_raw_measurements → location_mapping
        - Filas sin coordenadas se omiten (sesión fallida sin geo es correcto)
        - Usa la misma tabla location_mapping que el paso 6
        """,
    )


    # ========== PASO 8: CREAR VISTA MÓVIL ==========

    def run_create_mobile_dashboards():
        """Crear vista de dashboard para datos móviles"""
        from app.data_to_server.create_dashboard_view import create_dashboard_views
        create_dashboard_views()
        return "Vista de dashboard móvil creada con éxito"


    create_mobile_views = PythonOperator(
        task_id='create_mobile_views',
        python_callable=run_create_mobile_dashboards,
        dag=dag,
        doc_md="""
        ### PASO 8: Crear Vista de Dashboard - Datos Móviles

        - data_dashboard_view_visualization
        - Preserva todos los nombres de columnas originales
        - Lista para análisis directo en PowerBI
        """,
    )


    # ========== PASO 9: CREAR VISTA DE VOZ ==========

    def run_create_voice_dashboards():
        """Crear vista de dashboard para datos de voz"""
        from app.data_to_server.create_dashboard_view_voice import create_voice_dashboard_views
        create_voice_dashboard_views()
        return "Vista de dashboard de voz creada con éxito"


    create_voice_views = PythonOperator(
        task_id='create_voice_views',
        python_callable=run_create_voice_dashboards,
        dag=dag,
        doc_md="""
        ### PASO 9: Crear Vista de Dashboard - Datos de Voz

        - voice_dashboard_view_visualization
        - Preserva todos los nombres de columnas originales
        - Lista para análisis directo en PowerBI
        """,
    )


    # ========== PASO 10 [NUEVO]: CREAR VISTAS GRAFANA GEOREFERENCIADAS ==========

    def run_create_grafana_geo_views():
        """Crear vistas georeferenciadas para los 4 dashboards de Grafana"""
        from app.data_to_server.create_grafana_geo_views import create_grafana_geo_views
        create_grafana_geo_views()
        return "Vistas georeferenciadas para Grafana creadas con éxito"


    create_grafana_geo_views_task = PythonOperator(
        task_id='create_grafana_geo_views',
        python_callable=run_create_grafana_geo_views,
        dag=dag,
        doc_md="""
        ### PASO 10: Crear Vistas Georeferenciadas para Grafana

        | Vista                    | Fuente                   | Filtro aplicado          |
        |--------------------------|--------------------------|--------------------------|
        | grafana_mobile_geo_view  | mobile_raw_measurements  | SessionType HTTP Post/DL |
        | grafana_voice_geo_view   | voice_raw_measurements   | CallDirection = 'MO'     |

        Añade sobre los datos raw: Provincia, Cantón, Parroquia, PhoneNumber, Device, CZO.
        """,
    )


    # ========== PASO 11: VALIDACIÓN FINAL ==========

    def validate_complete_pipeline():
        """Validar integridad completa del pipeline"""
        import psycopg2
        import os
        from dotenv import load_dotenv

        load_dotenv()

        try:
            db_host = os.getenv('POSTGRES_HOST')
            db_port = os.getenv('POSTGRES_PORT')
            db_name = os.getenv('POSTGRES_DB')
            db_user = os.getenv('POSTGRES_USER')
            db_pass = os.getenv('POSTGRES_PASSWORD')

            print(
                f"DEBUG - Host: {db_host}, Port: {db_port}, DB: {db_name}, User: {db_user}, Pass: {'***' if db_pass else 'None'}")

            conn = psycopg2.connect(
                host=db_host,
                port=db_port,
                database=db_name,
                user=db_user,
                password=db_pass
            )

            validation_results = []

            with conn.cursor() as cur:
                # Validar datos móviles clean
                cur.execute("SELECT COUNT(*) FROM mobile_measurements WHERE is_current = 1")
                mobile_count = cur.fetchone()[0]

                # Validar datos de voz clean
                cur.execute("SELECT COUNT(*) FROM voice_measurements WHERE is_current = 1")
                voice_count = cur.fetchone()[0]

                # Validar regiones geográficas
                cur.execute("SELECT COUNT(*) FROM geographic_regions WHERE is_current = 1")
                regions_count = cur.fetchone()[0]

                # Validar mapeos espaciales (clean)
                cur.execute("""
                    SELECT COUNT(*) FROM location_mapping lm 
                    JOIN mobile_measurements mm ON lm.measurement_id = mm.measurement_id 
                    WHERE mm.is_current = 1
                """)
                mobile_mapped = cur.fetchone()[0]

                cur.execute("""
                    SELECT COUNT(*) FROM location_mapping lm 
                    JOIN voice_measurements vm ON lm.measurement_id = vm.measurement_id 
                    WHERE vm.is_current = 1
                """)
                voice_mapped = cur.fetchone()[0]

                # Validar vistas PowerBI
                cur.execute(
                    "SELECT EXISTS (SELECT FROM information_schema.views WHERE table_name = 'data_dashboard_view_visualization')")
                mobile_view_exists = cur.fetchone()[0]

                cur.execute(
                    "SELECT EXISTS (SELECT FROM information_schema.views WHERE table_name = 'voice_dashboard_view_visualization')")
                voice_view_exists = cur.fetchone()[0]

                # Contar registros en vistas PowerBI
                mobile_view_count = 0
                voice_view_count = 0

                if mobile_view_exists:
                    cur.execute("SELECT COUNT(*) FROM data_dashboard_view_visualization")
                    mobile_view_count = cur.fetchone()[0]

                if voice_view_exists:
                    cur.execute("SELECT COUNT(*) FROM voice_dashboard_view_visualization")
                    voice_view_count = cur.fetchone()[0]

                # [NUEVO] Validar datos raw
                cur.execute("SELECT COUNT(*) FROM mobile_raw_measurements WHERE is_current = 1")
                mobile_raw_count = cur.fetchone()[0]

                cur.execute("SELECT COUNT(*) FROM voice_raw_measurements WHERE is_current = 1")
                voice_raw_count = cur.fetchone()[0]

                # [NUEVO] Validar catálogo de dispositivos
                cur.execute(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'datasource_phones')")
                ds_table_exists = cur.fetchone()[0]
                ds_count = 0
                if ds_table_exists:
                    cur.execute("SELECT COUNT(*) FROM datasource_phones")
                    ds_count = cur.fetchone()[0]

                # [NUEVO] Validar vistas Grafana
                cur.execute(
                    "SELECT EXISTS (SELECT FROM information_schema.views WHERE table_name = 'grafana_mobile_geo_view')")
                grafana_mobile_exists = cur.fetchone()[0]

                cur.execute(
                    "SELECT EXISTS (SELECT FROM information_schema.views WHERE table_name = 'grafana_voice_geo_view')")
                grafana_voice_exists = cur.fetchone()[0]

                grafana_mobile_count = 0
                grafana_voice_count = 0

                if grafana_mobile_exists:
                    cur.execute("SELECT COUNT(*) FROM grafana_mobile_geo_view")
                    grafana_mobile_count = cur.fetchone()[0]

                if grafana_voice_exists:
                    cur.execute("SELECT COUNT(*) FROM grafana_voice_geo_view")
                    grafana_voice_count = cur.fetchone()[0]

                # Calcular porcentajes de mapeo
                mobile_mapping_pct = (mobile_mapped / mobile_count * 100) if mobile_count > 0 else 0
                voice_mapping_pct = (voice_mapped / voice_count * 100) if voice_count > 0 else 0

                # Generar reporte de validación
                validation_results = [
                    "=== VALIDACIÓN COMPLETA DEL PIPELINE ===",
                    f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    "",
                    "📊 DATOS PROCESADOS (clean / PowerBI):",
                    f"  • Mediciones móviles: {mobile_count:,}",
                    f"  • Mediciones de voz: {voice_count:,}",
                    f"  • Regiones geográficas: {regions_count:,}",
                    "",
                    "📋 DATOS RAW (regulatorio / Grafana):",
                    f"  • Mediciones móviles raw: {mobile_raw_count:,}",
                    f"  • Mediciones de voz raw: {voice_raw_count:,}",
                    "",
                    "📱 CATÁLOGO DE DISPOSITIVOS:",
                    f"  • datasource_phones: {ds_count:,} {'✅' if ds_count > 0 else '❌'}",
                    "",
                    "🗺️ MAPEOS ESPACIALES:",
                    f"  • Móviles mapeados: {mobile_mapped:,}",
                    f"  • Voz mapeados: {voice_mapped:,}",
                    f"  • Total mappings: {mobile_mapped + voice_mapped:,}",
                    "",
                    "📈 VISTAS POWERBI:",
                    f"  • Vista móvil: {'✅' if mobile_view_exists else '❌'} ({mobile_view_count:,} registros)",
                    f"  • Vista voz: {'✅' if voice_view_exists else '❌'} ({voice_view_count:,} registros)",
                    "",
                    "📡 VISTAS GRAFANA:",
                    f"  • grafana_mobile_geo_view: {'✅' if grafana_mobile_exists else '❌'} ({grafana_mobile_count:,} registros)",
                    f"  • grafana_voice_geo_view:  {'✅' if grafana_voice_exists else '❌'} ({grafana_voice_count:,} registros)",
                    "",
                    "🎯 CALIDAD DE DATOS:",
                    f"  • Mapeo móvil: {mobile_mapping_pct:.1f}%",
                    f"  • Mapeo voz: {voice_mapping_pct:.1f}%",
                    "",
                ]

                # Estado final
                pipeline_success = (
                        mobile_count > 0 and
                        voice_count > 0 and
                        mobile_view_exists and
                        voice_view_exists and
                        mobile_mapping_pct > 50 and
                        voice_mapping_pct > 50
                )

                if pipeline_success:
                    validation_results.extend([
                        "✅ ESTADO: Pipeline completado exitosamente",
                        "🚀 Datos listos para PowerBI y Grafana",
                        "",
                        "📋 CONEXIONES POWERBI:",
                        "  • data_dashboard_view_visualization (datos móviles)",
                        "  • voice_dashboard_view_visualization (datos de voz)",
                        "",
                        "📡 CONEXIONES GRAFANA:",
                        "  • grafana_mobile_geo_view (HTTP Post / HTTP Download)",
                        "  • grafana_voice_geo_view  (llamadas MO)"
                    ])
                else:
                    validation_results.append("❌ ESTADO: Pipeline completado con advertencias")
                    if mobile_count == 0:
                        validation_results.append("   ⚠️ No hay datos móviles")
                    if voice_count == 0:
                        validation_results.append("   ⚠️ No hay datos de voz")
                    if not mobile_view_exists:
                        validation_results.append("   ⚠️ Vista móvil no creada")
                    if not voice_view_exists:
                        validation_results.append("   ⚠️ Vista de voz no creada")
                    if mobile_mapping_pct <= 50:
                        validation_results.append(f"   ⚠️ Mapeo móvil bajo: {mobile_mapping_pct:.1f}%")
                    if voice_mapping_pct <= 50:
                        validation_results.append(f"   ⚠️ Mapeo voz bajo: {voice_mapping_pct:.1f}%")
                    if not grafana_mobile_exists:
                        validation_results.append("   ⚠️ Vista Grafana móvil no creada")
                    if not grafana_voice_exists:
                        validation_results.append("   ⚠️ Vista Grafana voz no creada")

                validation_text = "\n".join(validation_results)
                print(validation_text)

            conn.close()
            return validation_text

        except Exception as e:
            error_msg = f"❌ Error en validación final: {e}"
            print(error_msg)
            raise Exception(error_msg)


    pipeline_validation = PythonOperator(
        task_id='pipeline_validation',
        python_callable=validate_complete_pipeline,
        dag=dag,
        doc_md="""
        ### PASO 11: Validación Final del Pipeline

        Última etapa del pipeline secuencial:
        - Valida integridad de datos clean (PowerBI) y raw (Grafana)
        - Verifica mapeos espaciales
        - Confirma disponibilidad de todas las vistas
        - Genera reporte completo
        """,
    )

    # ========== DEFINICIÓN DE DEPENDENCIAS SECUENCIALES ==========

    # Pipeline completamente secuencial: 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9 → 10 → 11
    (
            process_mobile_data
            >> process_voice_data
            >> process_raw_mobile_data
            >> process_raw_voice_data
            >> load_datasource_task
            >> update_all_mappings
            >> update_raw_mappings_task
            >> create_mobile_views
            >> create_voice_views
            >> create_grafana_geo_views_task
            >> pipeline_validation
    )
