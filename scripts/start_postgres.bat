@echo off
echo Iniciando PostgreSQL...
"C:\Program Files\PostgreSQL\17\bin\pg_ctl" -D "C:\PostgreSQL\data" -l "C:\PostgreSQL\log.txt" start
if %ERRORLEVEL% EQU 0 (
  echo PostgreSQL iniciado correctamente.
) else (
  echo Error al iniciar PostgreSQL. Verifique los logs en C:\PostgreSQL\log.txt
)