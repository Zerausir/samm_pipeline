from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

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
        description='Pipeline secuencial completo para procesar datos móviles y de voz para visualización en PowerBI',
        schedule_interval='0 9,14 * * *',  # Ejecutar a las 9:00 y 14:00 diariamente
        catchup=False,
        tags=['etl', 'powerbi', 'mobile', 'voice', 'sequential']
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


    # ========== PASO 3: MAPEO ESPACIAL COMPLETO ==========

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
        ### PASO 3: Actualizar Mapeo Espacial Completo

        Tercera etapa del pipeline secuencial:
        - Procesa coordenadas de AMBOS tipos de datos
        - Mapea puntos a regiones geográficas
        - Actualiza tabla location_mapping para móviles Y voz
        - Usa métodos especializados para cada tipo
        """,
    )


    # ========== PASO 4: CREAR VISTA MÓVIL ==========

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
        ### PASO 4: Crear Vista de Dashboard - Datos Móviles

        Cuarta etapa del pipeline secuencial:
        - data_dashboard_view_visualization
        - Preserva todos los nombres de columnas originales
        - Lista para análisis directo en PowerBI
        """,
    )


    # ========== PASO 5: CREAR VISTA DE VOZ ==========

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
        ### PASO 5: Crear Vista de Dashboard - Datos de Voz

        Quinta etapa del pipeline secuencial:
        - voice_dashboard_view_visualization
        - Preserva todos los nombres de columnas originales
        - Lista para análisis directo en PowerBI
        """,
    )


    # ========== PASO 6: VALIDACIÓN FINAL ==========

    def validate_complete_pipeline():
        """Validar integridad completa del pipeline"""
        import psycopg2
        import os
        from dotenv import load_dotenv

        load_dotenv()

        try:
            # FIXED: Use environment variables that should now be available
            # Debug: Print what env vars we're getting
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
                # Validar datos móviles
                cur.execute("SELECT COUNT(*) FROM mobile_measurements WHERE is_current = 1")
                mobile_count = cur.fetchone()[0]

                # Validar datos de voz
                cur.execute("SELECT COUNT(*) FROM voice_measurements WHERE is_current = 1")
                voice_count = cur.fetchone()[0]

                # Validar regiones geográficas
                cur.execute("SELECT COUNT(*) FROM geographic_regions WHERE is_current = 1")
                regions_count = cur.fetchone()[0]

                # Validar mapeos espaciales
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

                # Validar vistas
                cur.execute(
                    "SELECT EXISTS (SELECT FROM information_schema.views WHERE table_name = 'data_dashboard_view_visualization')")
                mobile_view_exists = cur.fetchone()[0]

                cur.execute(
                    "SELECT EXISTS (SELECT FROM information_schema.views WHERE table_name = 'voice_dashboard_view_visualization')")
                voice_view_exists = cur.fetchone()[0]

                # Contar registros en vistas
                mobile_view_count = 0
                voice_view_count = 0

                if mobile_view_exists:
                    cur.execute("SELECT COUNT(*) FROM data_dashboard_view_visualization")
                    mobile_view_count = cur.fetchone()[0]

                if voice_view_exists:
                    cur.execute("SELECT COUNT(*) FROM voice_dashboard_view_visualization")
                    voice_view_count = cur.fetchone()[0]

                # Generar reporte de validación
                validation_results = [
                    "=== VALIDACIÓN COMPLETA DEL PIPELINE ===",
                    f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    "",
                    "📊 DATOS PROCESADOS:",
                    f"  • Mediciones móviles: {mobile_count:,}",
                    f"  • Mediciones de voz: {voice_count:,}",
                    f"  • Regiones geográficas: {regions_count:,}",
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
                    "🎯 CALIDAD DE DATOS:",
                ]

                # Calcular porcentajes de mapeo
                mobile_mapping_pct = (mobile_mapped / mobile_count * 100) if mobile_count > 0 else 0
                voice_mapping_pct = (voice_mapped / voice_count * 100) if voice_count > 0 else 0

                validation_results.extend([
                    f"  • Mapeo móvil: {mobile_mapping_pct:.1f}%",
                    f"  • Mapeo voz: {voice_mapping_pct:.1f}%",
                    "",
                ])

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
                        "🚀 Datos listos para PowerBI",
                        "",
                        "📋 CONEXIONES POWERBI:",
                        "  • data_dashboard_view_visualization (datos móviles)",
                        "  • voice_dashboard_view_visualization (datos de voz)"
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
        ### PASO 6: Validación Final del Pipeline

        Sexta y última etapa del pipeline secuencial:
        - Valida integridad de AMBOS tipos de datos
        - Verifica mapeos espaciales
        - Confirma disponibilidad de vistas
        - Genera reporte completo para PowerBI
        """,
    )

    # ========== DEFINICIÓN DE DEPENDENCIAS SECUENCIALES ==========

    # Pipeline completamente secuencial: 1 → 2 → 3 → 4 → 5 → 6
    process_mobile_data >> process_voice_data >> update_all_mappings >> create_mobile_views >> create_voice_views >> pipeline_validation
