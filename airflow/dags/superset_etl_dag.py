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
        description='Pipeline secuencial para procesar datos móviles y de voz (raw) para visualización en PowerBI',
        schedule='0 9,14 * * *',
        catchup=False,
        tags=['etl', 'powerbi', 'mobile', 'voice', 'sequential']
) as dag:
    # ========== PASO 1: CARGAR REGIONES GEOGRÁFICAS ==========

    def run_load_geographic_regions():
        """Carga el shapefile en geographic_regions solo si la tabla está vacía."""
        from app.data_to_server.load_geographic_regions import main
        return main()


    load_geographic_regions_task = PythonOperator(
        task_id='load_geographic_regions',
        python_callable=run_load_geographic_regions,
        dag=dag,
        doc_md="""
        ### PASO 1: Cargar Regiones Geográficas

        - Lee el shapefile de parroquias del Ecuador desde app/data/states/shapefile.shp
        - Inserta en geographic_regions solo si la tabla está vacía (idempotente)
        - 1 081 parroquias con geometría GeoJSON, provincia, cantón y parroquia
        - Si ya hay datos → se omite sin error

        Prerequisito: shapefile presente en /opt/airflow/app/data/states/
        """,
    )


    # ========== PASO 2: PROCESAR DATOS MÓVILES RAW ==========

    def run_raw_mobile_script():
        """Procesar datos móviles crudos — incluye sesiones fallidas y todos los SessionType"""
        from app.data_to_server.main_raw_mobile import main
        main()
        return "Procesamiento de datos móviles raw completado"


    process_raw_mobile_data = PythonOperator(
        task_id='process_raw_mobile_data',
        python_callable=run_raw_mobile_script,
        dag=dag,
        doc_md="""
        ### PASO 2: Procesar Datos Móviles Raw

        - Carga extract_datos_table1.parquet y extract_datos_table2.parquet
        - Sin dropna de coordenadas ni throughput — sesiones fallidas son datos regulatorios válidos
        - Todos los SessionType incluidos — la vista filtra HTTP Post/Download
        - Almacena en mobile_raw_measurements (ON CONFLICT DO NOTHING)
        """,
    )


    # ========== PASO 3: PROCESAR DATOS DE VOZ RAW ==========

    def run_raw_voice_script():
        """Procesar datos de voz crudos — incluye llamadas fallidas, bloqueadas y caídas"""
        from app.data_to_server.main_raw_voice import main
        main()
        return "Procesamiento de datos de voz raw completado"


    process_raw_voice_data = PythonOperator(
        task_id='process_raw_voice_data',
        python_callable=run_raw_voice_script,
        dag=dag,
        doc_md="""
        ### PASO 3: Procesar Datos de Voz Raw

        - Carga extract_voz_table1.parquet, extract_voz_table3.parquet, extract_voz_table4.parquet
        - Sin dropna — llamadas fallidas/bloqueadas/caídas son datos regulatorios válidos
        - Sin filtro de CallDirection — la vista filtra CallDirection='MO'
        - Almacena en voice_raw_measurements (ON CONFLICT DO NOTHING)
        """,
    )


    # ========== PASO 4: CARGAR CATÁLOGO DE DISPOSITIVOS ==========

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
        ### PASO 4: Cargar Catálogo de Dispositivos

        - Lee extract_datasource.parquet (PhoneNumber, IMSI, IMEI)
        - Enriquece con Device y CZO desde sense_nacional_v0.xlsx
        - Normaliza formato de número (+593XXXXXXXXX → 0XXXXXXXXX)
        - Upsert en datasource_phones (ON CONFLICT DO NOTHING)
        - Inserta registros sense-only sin entrada en Datasource SQL Server

        Prerequisito: extract_data_datasource.py ejecutado en máquina AD
        y el Parquet copiado a app/data/.
        """,
    )


    # ========== PASO 5: MAPEO ESPACIAL RAW ==========

    def run_update_raw_mappings():
        """
        Actualizar mapeo espacial para datos raw.

        Con los pasos clean eliminados, este paso es el único que pobla
        location_mapping. Cubre la totalidad de measurement_ids presentes
        en mobile_raw_measurements y voice_raw_measurements, incluyendo
        sesiones fallidas, bloqueadas y caídas que antes quedaban sin mapear.
        """
        from app.data_to_server.update_raw_mapping import update_raw_mappings
        count = update_raw_mappings()
        return f"Mapeo espacial raw completado: {count} mappings creados"


    update_raw_mappings_task = PythonOperator(
        task_id='update_raw_mappings',
        python_callable=run_update_raw_mappings,
        dag=dag,
        doc_md="""
        ### PASO 5: Mapeo Espacial — Datos Raw

        - mobile_raw_measurements + voice_raw_measurements → location_mapping
        - Registros sin coordenadas (sesión fallida sin GPS) se omiten correctamente
        - Usa geometry.covers(point) para correcta asignación en fronteras de parroquia
        - Al ser el único paso de mapeo, todos los measurement_id reciben
          location_type = 'mobile_raw' o 'voice_raw' de forma consistente
        """,
    )


    # ========== PASO 6: CREAR VISTAS GEOREFERENCIADAS PARA POWERBI ==========

    def run_create_geo_views():
        """Crear vistas georeferenciadas para los dashboards de PowerBI"""
        from app.data_to_server.create_grafana_geo_views import create_grafana_geo_views
        create_grafana_geo_views()
        return "Vistas georeferenciadas para PowerBI creadas con éxito"


    create_geo_views_task = PythonOperator(
        task_id='create_geo_views',
        python_callable=run_create_geo_views,
        dag=dag,
        doc_md="""
        ### PASO 6: Crear Vistas Georeferenciadas para PowerBI

        | Vista                   | Fuente                  | Filtro en vista          |
        |-------------------------|-------------------------|--------------------------|
        | grafana_mobile_geo_view | mobile_raw_measurements | SessionType HTTP Post/DL |
        | grafana_voice_geo_view  | voice_raw_measurements  | CallDirection = 'MO'     |

        Añade sobre los datos raw: Provincia, Cantón, Parroquia, PhoneNumber, Device, CZO.

        Puntos de falla (sesiones sin ThroughputMbps, llamadas caídas/bloqueadas/fallidas)
        quedan incluidos — PowerBI puede visualizarlos en mapas junto a los exitosos.
        """,
    )


    # ========== PASO 7: VALIDACIÓN FINAL ==========

    def validate_pipeline():
        """Validar integridad del pipeline raw"""
        import psycopg2
        import os
        from dotenv import load_dotenv
        from datetime import datetime

        load_dotenv()

        try:
            conn = psycopg2.connect(
                host=os.getenv('POSTGRES_HOST'),
                port=os.getenv('POSTGRES_PORT'),
                database=os.getenv('POSTGRES_DB'),
                user=os.getenv('POSTGRES_USER'),
                password=os.getenv('POSTGRES_PASSWORD'),
            )

            with conn.cursor() as cur:

                # --- Tablas raw ---
                cur.execute("SELECT COUNT(*) FROM mobile_raw_measurements WHERE is_current = 1")
                mobile_raw_count = cur.fetchone()[0]

                cur.execute("SELECT COUNT(*) FROM voice_raw_measurements WHERE is_current = 1")
                voice_raw_count = cur.fetchone()[0]

                # --- Regiones geográficas ---
                cur.execute("SELECT COUNT(*) FROM geographic_regions WHERE is_current = 1")
                regions_count = cur.fetchone()[0]

                # --- Catálogo de dispositivos ---
                cur.execute(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'datasource_phones')"
                )
                ds_exists = cur.fetchone()[0]
                ds_count = 0
                if ds_exists:
                    cur.execute("SELECT COUNT(*) FROM datasource_phones")
                    ds_count = cur.fetchone()[0]

                # --- Mapeos espaciales (sobre tablas raw) ---
                cur.execute("""
                    SELECT COUNT(*) FROM location_mapping lm
                    JOIN mobile_raw_measurements m ON lm.measurement_id = m.measurement_id
                    WHERE m.is_current = 1
                """)
                mobile_mapped = cur.fetchone()[0]

                cur.execute("""
                    SELECT COUNT(*) FROM location_mapping lm
                    JOIN voice_raw_measurements v ON lm.measurement_id = v.measurement_id
                    WHERE v.is_current = 1
                """)
                voice_mapped = cur.fetchone()[0]

                mobile_map_pct = (mobile_mapped / mobile_raw_count * 100) if mobile_raw_count > 0 else 0
                voice_map_pct = (voice_mapped / voice_raw_count * 100) if voice_raw_count > 0 else 0

                # --- Vistas georeferenciadas ---
                cur.execute(
                    "SELECT EXISTS (SELECT FROM information_schema.views WHERE table_name = 'grafana_mobile_geo_view')"
                )
                mobile_view_exists = cur.fetchone()[0]

                cur.execute(
                    "SELECT EXISTS (SELECT FROM information_schema.views WHERE table_name = 'grafana_voice_geo_view')"
                )
                voice_view_exists = cur.fetchone()[0]

                mobile_view_count = 0
                voice_view_count = 0
                if mobile_view_exists:
                    cur.execute("SELECT COUNT(*) FROM grafana_mobile_geo_view")
                    mobile_view_count = cur.fetchone()[0]
                if voice_view_exists:
                    cur.execute("SELECT COUNT(*) FROM grafana_voice_geo_view")
                    voice_view_count = cur.fetchone()[0]

                # --- Cobertura geográfica en vistas ---
                mobile_geo_pct = 0
                voice_geo_pct = 0
                if mobile_view_exists and mobile_view_count > 0:
                    cur.execute('SELECT COUNT(*) FROM grafana_mobile_geo_view WHERE "Provincia" IS NOT NULL')
                    mobile_geo_pct = cur.fetchone()[0] / mobile_view_count * 100
                if voice_view_exists and voice_view_count > 0:
                    cur.execute('SELECT COUNT(*) FROM grafana_voice_geo_view WHERE "Provincia" IS NOT NULL')
                    voice_geo_pct = cur.fetchone()[0] / voice_view_count * 100

            conn.close()

            # --- Criterio de éxito ---
            pipeline_ok = (
                    mobile_raw_count > 0
                    and voice_raw_count > 0
                    and mobile_view_exists
                    and voice_view_exists
                    and mobile_map_pct > 50
                    and voice_map_pct > 50
            )

            lines = [
                "=" * 55,
                "   VALIDACIÓN DEL PIPELINE ETL — SAMM",
                f"   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "=" * 55,
                "",
                "📋 DATOS RAW (PowerBI):",
                f"  • mobile_raw_measurements : {mobile_raw_count:,}",
                f"  • voice_raw_measurements  : {voice_raw_count:,}",
                "",
                "🗺️  REGIONES GEOGRÁFICAS:",
                f"  • geographic_regions      : {regions_count:,} parroquias",
                "",
                "📱 CATÁLOGO DE DISPOSITIVOS:",
                f"  • datasource_phones       : {ds_count:,} {'✅' if ds_count > 0 else '❌'}",
                "",
                "📍 MAPEO ESPACIAL (raw):",
                f"  • Móviles mapeados : {mobile_mapped:,} / {mobile_raw_count:,} ({mobile_map_pct:.1f}%)",
                f"  • Voz mapeada      : {voice_mapped:,} / {voice_raw_count:,} ({voice_map_pct:.1f}%)",
                "",
                "📊 VISTAS POWERBI:",
                f"  • grafana_mobile_geo_view : {'✅' if mobile_view_exists else '❌'} "
                f"{mobile_view_count:,} registros — {mobile_geo_pct:.1f}% con Provincia",
                f"  • grafana_voice_geo_view  : {'✅' if voice_view_exists else '❌'} "
                f"{voice_view_count:,} registros — {voice_geo_pct:.1f}% con Provincia",
                "",
            ]

            if pipeline_ok:
                lines += [
                    "✅ ESTADO: Pipeline completado exitosamente",
                    "",
                    "🔌 CONEXIONES POWERBI:",
                    "  • grafana_mobile_geo_view  (HTTP Post + HTTP Download + fallos)",
                    "  • grafana_voice_geo_view   (llamadas MO + caídas + bloqueadas)",
                ]
            else:
                lines.append("❌ ESTADO: Pipeline completado con advertencias")
                if mobile_raw_count == 0:
                    lines.append("   ⚠️  No hay datos móviles raw")
                if voice_raw_count == 0:
                    lines.append("   ⚠️  No hay datos de voz raw")
                if not mobile_view_exists:
                    lines.append("   ⚠️  grafana_mobile_geo_view no creada")
                if not voice_view_exists:
                    lines.append("   ⚠️  grafana_voice_geo_view no creada")
                if mobile_map_pct <= 50:
                    lines.append(f"   ⚠️  Mapeo móvil bajo: {mobile_map_pct:.1f}%")
                if voice_map_pct <= 50:
                    lines.append(f"   ⚠️  Mapeo voz bajo: {voice_map_pct:.1f}%")

            report = "\n".join(lines)
            print(report)
            return report

        except Exception as e:
            msg = f"❌ Error en validación final: {e}"
            print(msg)
            raise Exception(msg)


    pipeline_validation = PythonOperator(
        task_id='pipeline_validation',
        python_callable=validate_pipeline,
        dag=dag,
        doc_md="""
        ### PASO 7: Validación Final del Pipeline

        Verifica integridad de:
        - Tablas raw (mobile_raw_measurements, voice_raw_measurements)
        - Regiones geográficas (geographic_regions)
        - Catálogo datasource_phones
        - Cobertura del mapeo espacial (umbral > 50%)
        - Existencia y recuento de vistas georeferenciadas
        - Porcentaje de registros con Provincia asignada en cada vista

        Criterio de éxito: datos raw > 0, ambas vistas existentes, mapeo > 50%.
        """,
    )

    # ========== DEPENDENCIAS SECUENCIALES ==========
    # 1 → 2 → 3 → 4 → 5 → 6 → 7
    (
            load_geographic_regions_task
            >> process_raw_mobile_data
            >> process_raw_voice_data
            >> load_datasource_task
            >> update_raw_mappings_task
            >> create_geo_views_task
            >> pipeline_validation
    )
