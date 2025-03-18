@echo off
echo Iniciando Apache Airflow webserver y scheduler...
cd /d "C:\Users\ivan.suarez\Documents\workspace\samm_plotly_dash"
call .venv\Scripts\activate

set AIRFLOW_HOME=C:\Users\ivan.suarez\Documents\workspace\samm_plotly_dash\airflow

REM Iniciar el webserver en una nueva ventana
start cmd /k "title Airflow Webserver && echo Iniciando Airflow Webserver... && airflow webserver --port 8080"

REM Esperar 5 segundos para que el webserver inicie
timeout /t 5 /nobreak > nul

REM Iniciar el scheduler en esta ventana
echo Iniciando Airflow Scheduler...
airflow scheduler