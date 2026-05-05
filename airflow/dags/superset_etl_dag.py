from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator

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
    # ========== PASO 0: VERIFICAR DATOS NUEVOS ==========

    def check_new_data() -> bool:
        """
        Compara los metadatos de los Parquets actuales contra el último
        pipeline exitoso almacenado en pipeline_state.

        Retorna True  → hay datos nuevos, continuar con el pipeline.
        Retorna False → sin cambios, ShortCircuitOperator omite pasos 1-7.

        Lógica de comparación (dos niveles):
          1. latest_record_date  — fecha del registro más reciente en SQL Server.
          2. row counts          — total de filas por tabla.
        Si cualquier campo difiere → procesar.
        Si pipeline_state está vacío (primera ejecución) → procesar.
        Si algún archivo de metadatos no existe → procesar (extractor no corrió aún).
        """
        import json
        import os
        import psycopg2
        from dotenv import load_dotenv

        load_dotenv()

        DATA_DIR = os.getenv('DATA_DIR', '/opt/airflow/app/data')

        # ------------------------------------------------------------------
        # 1. Leer metadatos actuales de los Parquets
        # ------------------------------------------------------------------
        metadata_files = {
            'datos': os.path.join(DATA_DIR, 'extraction_datos_metadata.json'),
            'voz': os.path.join(DATA_DIR, 'extraction_voz_metadata.json'),
            'datasource': os.path.join(DATA_DIR, 'extraction_datasource_metadata.json'),
        }

        current = {}
        for key, path in metadata_files.items():
            if not os.path.exists(path):
                print(f"⚠️  Metadatos '{key}' no encontrados en {path} → procesando")
                return True
            with open(path, 'r', encoding='utf-8') as f:
                current[key] = json.load(f)
            print(f"✅ Metadatos '{key}' cargados desde {path}")

        # Campos de comparación extraídos de los JSONs
        new_state = {
            'datos_latest_record_date': current['datos'].get('latest_record_date'),
            'datos_table1_rows': int(current['datos'].get('table1_rows', 0)),
            'datos_table2_rows': int(current['datos'].get('table2_rows', 0)),
            'voz_latest_record_date': current['voz'].get('latest_record_date'),
            'voz_table1_rows': int(current['voz'].get('table1_rows', 0)),
            'voz_table3_rows': int(current['voz'].get('table3_rows', 0)),
            'voz_table4_rows': int(current['voz'].get('table4_rows', 0)),
            'datasource_total_rows': int(current['datasource'].get('total_rows', 0)),
        }

        print("\n📋 Estado actual de los Parquets:")
        for k, v in new_state.items():
            print(f"   {k}: {v}")

        # ------------------------------------------------------------------
        # 2. Conectar a PostgreSQL y garantizar que pipeline_state existe
        # ------------------------------------------------------------------
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT'),
            database=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
        )

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS pipeline_state (
                        id                        SERIAL PRIMARY KEY,
                        run_timestamp             TIMESTAMP NOT NULL DEFAULT NOW(),
                        datos_latest_record_date  TEXT,
                        datos_table1_rows         BIGINT,
                        datos_table2_rows         BIGINT,
                        voz_latest_record_date    TEXT,
                        voz_table1_rows           BIGINT,
                        voz_table3_rows           BIGINT,
                        voz_table4_rows           BIGINT,
                        datasource_total_rows     BIGINT,
                        status                    TEXT DEFAULT 'success'
                    )
                """)
                conn.commit()

                # ------------------------------------------------------------------
                # 3. Leer último estado exitoso
                # ------------------------------------------------------------------
                cur.execute("""
                    SELECT
                        datos_latest_record_date,
                        datos_table1_rows,
                        datos_table2_rows,
                        voz_latest_record_date,
                        voz_table1_rows,
                        voz_table3_rows,
                        voz_table4_rows,
                        datasource_total_rows
                    FROM pipeline_state
                    WHERE status = 'success'
                    ORDER BY run_timestamp DESC
                    LIMIT 1
                """)
                row = cur.fetchone()

        finally:
            conn.close()

        # ------------------------------------------------------------------
        # 4. Comparar
        # ------------------------------------------------------------------
        if row is None:
            print("\n🆕 pipeline_state vacío — primera ejecución → procesando")
            return True

        last_state = {
            'datos_latest_record_date': row[0],
            'datos_table1_rows': row[1],
            'datos_table2_rows': row[2],
            'voz_latest_record_date': row[3],
            'voz_table1_rows': row[4],
            'voz_table3_rows': row[5],
            'voz_table4_rows': row[6],
            'datasource_total_rows': row[7],
        }

        print("\n📋 Último estado procesado:")
        for k, v in last_state.items():
            print(f"   {k}: {v}")

        diffs = [k for k in new_state if str(new_state[k]) != str(last_state.get(k))]

        if diffs:
            print(f"\n🔄 Cambios detectados en: {', '.join(diffs)} → procesando")
            return True

        print("\n✅ Sin datos nuevos — pipeline omitido (ahorro de recursos)")
        return False


    check_new_data_task = ShortCircuitOperator(
        task_id='check_new_data',
        python_callable=check_new_data,
        dag=dag,
        doc_md="""
        ### PASO 0: Verificar Datos Nuevos

        Compara los metadatos de los Parquets actuales contra el último
        pipeline exitoso registrado en `pipeline_state` (PostgreSQL).

        **Campos comparados:**
        - `latest_record_date` de datos móviles y de voz
        - Conteo de filas por tabla (`table1_rows`, `table2_rows`, etc.)
        - `total_rows` del catálogo de dispositivos

        **Resultado:**
        - `True`  → hay cambios → el pipeline continúa con los pasos 1-7
        - `False` → sin cambios → todos los pasos downstream pasan a `skipped`

        La tabla `pipeline_state` se crea automáticamente si no existe
        (primera ejecución). Los metadatos son generados por `samm_extract_data`
        y transferidos a `app/data/` junto con los Parquets.
        """,
    )


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
        """
        Validar integridad del pipeline raw.
        Si la validación es exitosa, registra el estado en pipeline_state
        para que el Paso 0 pueda comparar en la próxima ejecución.
        """
        import json
        import os
        import psycopg2
        from dotenv import load_dotenv
        from datetime import datetime

        load_dotenv()

        DATA_DIR = os.getenv('DATA_DIR', '/opt/airflow/app/data')

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
                mobile_provincia_pct = 0.0
                voice_provincia_pct = 0.0

                if mobile_view_exists:
                    cur.execute('SELECT COUNT(*) FROM grafana_mobile_geo_view')
                    mobile_view_count = cur.fetchone()[0]
                    if mobile_view_count > 0:
                        cur.execute(
                            'SELECT COUNT(*) FROM grafana_mobile_geo_view WHERE "Provincia" IS NOT NULL'
                        )
                        mobile_provincia_pct = cur.fetchone()[0] / mobile_view_count * 100

                if voice_view_exists:
                    cur.execute('SELECT COUNT(*) FROM grafana_voice_geo_view')
                    voice_view_count = cur.fetchone()[0]
                    if voice_view_count > 0:
                        cur.execute(
                            'SELECT COUNT(*) FROM grafana_voice_geo_view WHERE "Provincia" IS NOT NULL'
                        )
                        voice_provincia_pct = cur.fetchone()[0] / voice_view_count * 100

            # --- Determinar éxito ---
            # pipeline_ok: criterio estricto para el reporte ✅/❌
            # Incluye umbral de mapeo de voz como métrica de calidad.
            pipeline_ok = (
                    mobile_raw_count > 0
                    and voice_raw_count > 0
                    and mobile_view_exists
                    and voice_view_exists
                    and mobile_map_pct > 50
                    and voice_map_pct > 50
            )

            # pipeline_ran: criterio para registrar en pipeline_state.
            # El mapeo de voz bajo (~21%) es esperado porque las llamadas
            # fallidas/bloqueadas/caídas no generan coordenadas GPS por diseño.
            # No debe bloquear el registro del estado procesado.
            pipeline_ran = (
                    mobile_raw_count > 0
                    and voice_raw_count > 0
                    and mobile_view_exists
                    and voice_view_exists
            )

            lines = [
                "",
                "=" * 70,
                "📊 REPORTE DE VALIDACIÓN — SAMM PIPELINE",
                "=" * 70,
                f"  Móvil raw       : {mobile_raw_count:,} registros",
                f"  Voz raw         : {voice_raw_count:,} registros",
                f"  Regiones        : {regions_count:,} parroquias",
                f"  Catálogo        : {ds_count:,} dispositivos",
                f"  Mapeo móvil     : {mobile_map_pct:.1f}%  ({mobile_mapped:,}/{mobile_raw_count:,})",
                f"  Mapeo voz       : {voice_map_pct:.1f}%  ({voice_mapped:,}/{voice_raw_count:,})",
                f"  Vista móvil     : {'✅' if mobile_view_exists else '❌'}  {mobile_view_count:,} registros  "
                f"({mobile_provincia_pct:.1f}% con Provincia)",
                f"  Vista voz       : {'✅' if voice_view_exists else '❌'}  {voice_view_count:,} registros  "
                f"({voice_provincia_pct:.1f}% con Provincia)",
                "=" * 70,
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

            # ------------------------------------------------------------------
            # Registrar estado en pipeline_state si el pipeline procesó datos.
            # Se usa pipeline_ran (no pipeline_ok) porque el mapeo bajo de voz
            # es esperado con llamadas fallidas/bloqueadas/caídas sin GPS.
            # El status refleja si hubo advertencias de calidad.
            # ------------------------------------------------------------------
            if pipeline_ran:
                metadata_files = {
                    'datos': os.path.join(DATA_DIR, 'extraction_datos_metadata.json'),
                    'voz': os.path.join(DATA_DIR, 'extraction_voz_metadata.json'),
                    'datasource': os.path.join(DATA_DIR, 'extraction_datasource_metadata.json'),
                }

                meta = {}
                for key, path in metadata_files.items():
                    if os.path.exists(path):
                        with open(path, 'r', encoding='utf-8') as f:
                            meta[key] = json.load(f)
                    else:
                        meta[key] = {}

                conn2 = psycopg2.connect(
                    host=os.getenv('POSTGRES_HOST'),
                    port=os.getenv('POSTGRES_PORT'),
                    database=os.getenv('POSTGRES_DB'),
                    user=os.getenv('POSTGRES_USER'),
                    password=os.getenv('POSTGRES_PASSWORD'),
                )
                try:
                    with conn2.cursor() as cur2:
                        cur2.execute("""
                            INSERT INTO pipeline_state (
                                run_timestamp,
                                datos_latest_record_date,
                                datos_table1_rows,
                                datos_table2_rows,
                                voz_latest_record_date,
                                voz_table1_rows,
                                voz_table3_rows,
                                voz_table4_rows,
                                datasource_total_rows,
                                status
                            ) VALUES (
                                NOW(),
                                %(datos_latest)s,
                                %(datos_t1)s,
                                %(datos_t2)s,
                                %(voz_latest)s,
                                %(voz_t1)s,
                                %(voz_t3)s,
                                %(voz_t4)s,
                                %(ds_rows)s,
                                %(status)s
                            )
                        """, {
                            'datos_latest': meta['datos'].get('latest_record_date'),
                            'datos_t1': int(meta['datos'].get('table1_rows', 0)),
                            'datos_t2': int(meta['datos'].get('table2_rows', 0)),
                            'voz_latest': meta['voz'].get('latest_record_date'),
                            'voz_t1': int(meta['voz'].get('table1_rows', 0)),
                            'voz_t3': int(meta['voz'].get('table3_rows', 0)),
                            'voz_t4': int(meta['voz'].get('table4_rows', 0)),
                            'ds_rows': int(meta['datasource'].get('total_rows', 0)),
                            'status': 'success' if pipeline_ok else 'warning',
                        })
                        conn2.commit()
                    print("\n✅ Estado registrado en pipeline_state")
                finally:
                    conn2.close()

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

        Criterio de calidad (`pipeline_ok`): datos raw > 0, ambas vistas existentes,
        mapeo móvil > 50% y mapeo voz > 50%. Determina el ✅/❌ del reporte.

        Criterio de registro (`pipeline_ran`): datos raw > 0 y ambas vistas existentes.
        El mapeo de voz bajo (~21%) es esperado — llamadas fallidas/bloqueadas/caídas
        no generan GPS por diseño regulatorio. No bloquea el registro de estado.

        **Siempre que `pipeline_ran` sea True**: registra en `pipeline_state` con
        `status='success'` o `status='warning'` según `pipeline_ok`. Esto permite
        al Paso 0 (`check_new_data`) omitir ejecuciones sin datos nuevos.
        """,
    )

    # ========== DEPENDENCIAS SECUENCIALES ==========
    # 0 → 1 → 2 → 3 → 4 → 5 → 6 → 7
    (
            check_new_data_task
            >> load_geographic_regions_task
            >> process_raw_mobile_data
            >> process_raw_voice_data
            >> load_datasource_task
            >> update_raw_mappings_task
            >> create_geo_views_task
            >> pipeline_validation
    )
