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
        description='Pipeline para procesar datos y visualizarlos en Superset',
        schedule_interval='0 9,14 * * *',  # Ejecutar a las 9:00 y 13:00 diariamente
        catchup=False,
) as dag:
    # Tarea para ejecutar main.py
    def run_main_script():
        from app.data_to_server.main import main
        main()
        return "Procesamiento de datos principal completado"


    process_data = PythonOperator(
        task_id='process_data',
        python_callable=run_main_script,
        dag=dag,
    )


    # Tarea para ejecutar update_mapping.py
    def run_update_mapping():
        from app.data_to_server.update_mapping import update_mapping
        update_mapping()
        return "Actualización de mapeo completada"


    update_mappings = PythonOperator(
        task_id='update_mappings',
        python_callable=run_update_mapping,
        dag=dag,
    )


    # Tarea para ejecutar create_dashboard_view.py
    def run_create_dashboards():
        from app.data_to_server.create_dashboard_view import create_dashboard_views
        create_dashboard_views()
        return "Vistas de dashboard creadas con éxito"


    create_views = PythonOperator(
        task_id='create_views',
        python_callable=run_create_dashboards,
        dag=dag,
    )

    # Definir las dependencias de tareas
    process_data >> update_mappings >> create_views
