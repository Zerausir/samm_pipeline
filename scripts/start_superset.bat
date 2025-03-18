@echo off
echo Iniciando Apache Superset...
cd /d "C:\Users\ivan.suarez\Documents\workspace\samm_plotly_dash"
call .venv\Scripts\activate
set SUPERSET_CONFIG_PATH=C:\Users\ivan.suarez\Documents\workspace\samm_plotly_dash\superset\superset_config.py
superset run -p 8088 --with-threads