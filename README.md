# samm_pipeline

[![Python 3.14](https://img.shields.io/badge/python-3.14-blue.svg)](https://www.python.org/)
[![Airflow 3.2.0](https://img.shields.io/badge/airflow-3.2.0-017CEE.svg)](https://airflow.apache.org/)
[![PostgreSQL 17](https://img.shields.io/badge/postgresql-17-336791.svg)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/docker-compose-2496ED.svg)](https://www.docker.com/)

Pipeline ETL para procesamiento automatizado de datos de telecomunicaciones móviles y de voz del sistema SAMM (Sistema
Automático de Medición de Redes Móviles), orquestado con Apache Airflow 3.2.0 y almacenado en PostgreSQL para análisis
en Grafana y PowerBI.

Prerequisito: [`samm_extract_data`](https://github.com/Zerausir/samm_extract_data) debe ejecutarse primero para generar
los archivos Parquet de entrada.

---

## Tabla de Contenidos

- [Descripción](#descripción)
- [Arquitectura](#arquitectura)
- [Requisitos](#requisitos)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Configuración](#configuración)
- [Instalación](#instalación)
- [Archivos de Datos Requeridos](#archivos-de-datos-requeridos)
- [Pipeline ETL — 7 Pasos](#pipeline-etl--7-pasos)
- [Base de Datos](#base-de-datos)
- [Vistas Disponibles](#vistas-disponibles)
- [Administración](#administración)
- [Carga Histórica Inicial](#carga-histórica-inicial)
- [Solución de Problemas](#solución-de-problemas)

---

## Descripción

Este pipeline recibe archivos Parquet generados por `samm_extract_data`, los transforma, enriquece con datos
geoespaciales, los almacena en PostgreSQL y crea vistas optimizadas para su consumo en Grafana y PowerBI. Corre
completamente dockerizado con Apache Airflow 3.2.0 como orquestador.

### Flujo completo de datos

```
[SQL Server SAMM] ──► [samm_extract_data] ──► [Archivos Parquet]
                          (VM3 — Windows AD)           │
                                                        ▼
                                              [samm_pipeline — VM2]
                                                        │
                                         ┌──────────────┼──────────────┐
                                         ▼              ▼              ▼
                                    Airflow DAG    PostgreSQL 17    Grafana / PowerBI
                                    (orquesta)    (VM1 — bare-metal) (visualiza)
```

---

## Arquitectura

### Infraestructura — 3 VMs

| VM  | Hostname       | IP             | Función                                     |
|-----|----------------|----------------|---------------------------------------------|
| VM1 | XXXXPBI1       | XXX.XXX.XXX.50 | PostgreSQL 17 bare-metal                    |
| VM2 | XXXXPBI2       | XXX.XXX.XXX.51 | Docker + Airflow (este repo)                |
| VM3 | *(Windows AD)* | XXX.XXX.XXX.52 | Extracción SQL Server (`samm_extract_data`) |

### Servicios Docker — VM2

| Servicio                | Imagen                 | Puerto | Función                       |
|-------------------------|------------------------|--------|-------------------------------|
| `airflow-webserver`     | `custom-airflow:3.2.0` | 8080   | API server + UI (Airflow 3.x) |
| `airflow-scheduler`     | `custom-airflow:3.2.0` | —      | Orquestador del DAG           |
| `airflow-triggerer`     | `custom-airflow:3.2.0` | —      | Tareas diferibles             |
| `airflow-dag-processor` | `custom-airflow:3.2.0` | —      | Parser de DAGs                |
| `airflow-init`          | `custom-airflow:3.2.0` | —      | Inicialización única          |
| `data-processor`        | `python:3.14-slim`     | —      | Contenedor auxiliar           |

> PostgreSQL **no** corre en Docker — está gestionado como servicio bare-metal en VM1.

### DAG: `superset_etl_pipeline`

Pipeline **completamente secuencial** — cada paso depende del anterior:

```
load_geographic_regions    (paso 1) — Shapefile → geographic_regions (solo si vacía)
        │
process_raw_mobile_data    (paso 2) — ETL móvil raw → mobile_raw_measurements
        │
process_raw_voice_data     (paso 3) — ETL voz raw → voice_raw_measurements
        │
load_datasource            (paso 4) — Catálogo → datasource_phones
        │
update_raw_mappings        (paso 5) — Mapeo espacial (tablas raw)
        │
create_geo_views           (paso 6) — Vistas Grafana/PowerBI georeferenciadas
        │
pipeline_validation        (paso 7) — Validación final del pipeline
```

**Horario**: diariamente a las 09:00 y 14:00 (zona horaria `America/Guayaquil`).

---

## Requisitos

### Software

- Docker Engine (Linux) o Docker Desktop (Windows)
- Docker Compose v2.14.0+

### Hardware mínimo — VM2

- 8 GB de RAM
- 4 vCPU
- 50 GB de espacio en disco

> ⚠️ Los contenedores de Airflow no tienen límites de memoria configurados — utilizan toda la RAM disponible del host.
> Para volúmenes de datos grandes (> 6 meses), ver [Carga Histórica Inicial](#carga-histórica-inicial).

---

## Estructura del Proyecto

```
samm_pipeline/
├── airflow/
│   ├── dags/
│   │   └── superset_etl_dag.py              # DAG principal — 7 pasos secuenciales
│   ├── logs/                                # Logs de ejecución (auto-generado, en .gitignore)
│   └── plugins/
│       └── custom_hooks.py                  # Hooks personalizados (Airflow 3.x SDK)
├── app/
│   ├── data/                                # Archivos Parquet y estáticos (en .gitignore)
│   │   ├── extract_datos_table1.parquet     # SessionSummary móvil
│   │   ├── extract_datos_table2.parquet     # SessionSummaryData móvil
│   │   ├── extract_voz_table1.parquet       # SessionSummary voz
│   │   ├── extract_voz_table3.parquet       # SessionSummaryVoice
│   │   ├── extract_voz_table4.parquet       # SessionVoiceQuality
│   │   ├── extract_datasource.parquet       # Catálogo PhoneNumber ↔ IMEI
│   │   ├── sense_nacional_v0.xlsx           # Catálogo dispositivos SENSE
│   │   └── states/                          # Shapefile de Ecuador (1 081 parroquias)
│   │       ├── shapefile.shp
│   │       ├── shapefile.shx
│   │       ├── shapefile.dbf
│   │       └── shapefile.prj
│   ├── data_to_server/
│   │   ├── load_geographic_regions.py       # Paso 1 — Shapefile → geographic_regions
│   │   ├── main_raw_mobile.py               # Paso 2 — ETL móvil raw
│   │   ├── main_raw_voice.py                # Paso 3 — ETL voz raw
│   │   ├── load_datasource.py               # Paso 4 — Catálogo dispositivos
│   │   ├── update_raw_mapping.py            # Paso 5 — Mapeo espacial raw
│   │   └── create_grafana_geo_views.py      # Paso 6 — Vistas Grafana/PowerBI
│   ├── tests/
│   │   ├── check_odbc.py
│   │   ├── table_access_test.py
│   │   ├── test_postgresql_connection.py
│   │   ├── test_postgis.py
│   │   └── test_samm_server_connection.py
│   └── utils/
│       ├── postgres_handler.py              # Handler PostgreSQL optimizado
│       └── spatial_mapper.py               # Mapeador espacial con STRtree R-tree
├── diagrama_arquitectura/                   # Diagramas HTML interactivos
├── excel_para_fabric/
│   └── transfer.py                          # Exportación a Excel (auxiliar)
├── nginx/
│   └── nginx.conf                           # Reverse proxy HTTPS (opcional)
├── postgres-conf/
│   └── postgresql.conf                      # Tuning PostgreSQL
├── scripts/
│   ├── start_airflow.bat
│   └── start_postgres.bat
├── Dockerfile.airflow                        # Imagen personalizada Airflow 3.2.0 / Python 3.14
├── docker-compose.yaml
├── requirements.txt
├── .env                                     # Variables de entorno (en .gitignore)
├── .env.example
└── .gitignore
```

---

## Configuración

### Variables de entorno

Crear `.env` en la raíz del proyecto:

```env
# ─── PostgreSQL (VM1 — XXXXPBI1) ──────────────────────────────────────────
POSTGRES_HOST=XXX.XXX.XXX.50
POSTGRES_PORT=5432
POSTGRES_DB=samm_db
POSTGRES_USER=samm_user
POSTGRES_PASSWORD=<contraseña>

# ─── Airflow ──────────────────────────────────────────────────────────────
AIRFLOW_UID=0
_AIRFLOW_WWW_USER_USERNAME=admin

# JWT para comunicación scheduler ↔ api-server (Airflow 3.x — obligatorio)
AIRFLOW__API_AUTH__JWT_SECRET=<jwt_secret_base64>

# Fernet key para cifrado de conexiones
AIRFLOW__CORE__FERNET_KEY=<fernet_key_base64>

# ─── Rutas de datos (dentro del contenedor) ───────────────────────────────
SHAPEFILE_PATH=/opt/airflow/app/data/states/shapefile.shp
```

> 🔒 El archivo `.env` está en `.gitignore`. Asegurar permisos con `chmod 600 .env`.

**Generar JWT secret (PowerShell):**

```powershell
$bytes = New-Object byte[] 32
[System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
[System.Convert]::ToBase64String($bytes)
```

**Generar Fernet key (Python):**

```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

---

## Instalación

### 1. Clonar el repositorio

```bash
git clone git@github.com:Zerausir/samm_pipeline.git
cd samm_pipeline
```

### 2. Configurar variables de entorno

```bash
nano .env
chmod 600 .env
```

### 3. Crear directorios necesarios

```bash
mkdir -p airflow/logs app/data/states
```

### 4. Construir imagen personalizada de Airflow

```bash
docker build -f Dockerfile.airflow -t custom-airflow:3.2.0 .
```

Dependencias incluidas: `numpy`, `asyncpg`, `psycopg2-binary`, `geopandas`, `pyodbc`, `environs`, `openpyxl`, `pykml`,
`sqlalchemy`, `pyarrow`.

### 5. Inicializar y arrancar servicios

```bash
# Inicializar base de datos de Airflow en VM1 (una sola vez)
docker compose up airflow-init

# Arrancar todos los servicios
docker compose up -d

# Verificar estado
docker compose ps
```

### 6. Obtener contraseña de Airflow

```bash
docker compose logs airflow-webserver | grep "Password for user"
```

> ⚠️ La contraseña cambia si el contenedor es recreado. Un simple `restart` la conserva.

### 7. Acceder a la UI

```
http://XXX.XXX.XXX.51:8080
```

### 8. Configurar auto-inicio (systemd — VM2)

```bash
sudo nano /etc/systemd/system/samm-pipeline.service
```

```ini
[Unit]
Description = SAMM Pipeline - Airflow Docker Compose
Requires = docker.service
After = docker.service network-online.target

[Service]
Type = oneshot
RemainAfterExit = yes
WorkingDirectory = /opt/samm_pipeline
ExecStart = /usr/bin/docker compose up -d --remove-orphans
ExecStop = /usr/bin/docker compose down
TimeoutStartSec = 300

[Install]
WantedBy = multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable samm-pipeline.service
```

### 9. Copiar archivos estáticos (one-time)

```bash
# Desde VM3 o PC local hacia VM2
scp -r states/ root@XXX.XXX.XXX.51:/opt/samm_pipeline/app/data/
scp sense_nacional_v0.xlsx root@XXX.XXX.XXX.51:/opt/samm_pipeline/app/data/
```

---

## Archivos de Datos Requeridos

Todos deben estar en `app/data/` antes de ejecutar el DAG:

| Archivo                                           | Origen                                             | Frecuencia     |
|---------------------------------------------------|----------------------------------------------------|----------------|
| `extract_datos_table1.parquet`                    | `samm_extract_data` → `extract_data_datos.py`      | Cada ejecución |
| `extract_datos_table2.parquet`                    | `samm_extract_data` → `extract_data_datos.py`      | Cada ejecución |
| `extract_voz_table1.parquet`                      | `samm_extract_data` → `extract_data_voz.py`        | Cada ejecución |
| `extract_voz_table3.parquet`                      | `samm_extract_data` → `extract_data_voz.py`        | Cada ejecución |
| `extract_voz_table4.parquet`                      | `samm_extract_data` → `extract_data_voz.py`        | Cada ejecución |
| `extract_datasource.parquet`                      | `samm_extract_data` → `extract_data_datasource.py` | Cada ejecución |
| `sense_nacional_v0.xlsx`                          | Provisto manualmente                               | Solo si cambia |
| `states/shapefile.shp` (+ `.shx`, `.dbf`, `.prj`) | Provisto manualmente                               | Una sola vez   |

La transferencia automática desde VM3 → VM2 se realiza via `deploy.py` + `run_and_deploy.bat` configurado en el
Programador de Tareas de Windows en VM3 (08:00 y 13:00).

---

## Pipeline ETL — 7 Pasos

### Paso 1 — `load_geographic_regions`

**Script**: `app/data_to_server/load_geographic_regions.py`

Carga el shapefile de parroquias del Ecuador en `geographic_regions`. Operación idempotente: si la tabla ya tiene datos
retorna inmediatamente. Solo crea la tabla `geographic_regions` — no crea ninguna otra tabla. Se ejecuta en menos de 1
segundo en ejecuciones normales.

### Paso 2 — `process_raw_mobile_data`

**Script**: `app/data_to_server/main_raw_mobile.py`

Carga `extract_datos_table1.parquet` y `extract_datos_table2.parquet`. Normaliza tipos, elimina duplicados, merge
chunked (10 000 filas/chunk), calcula `ThroughputMbps` y almacena **todos** los `SessionType` en
`mobile_raw_measurements` sin filtrar sesiones fallidas ni registros sin coordenadas. Los filtros se aplican en las
vistas.

### Paso 3 — `process_raw_voice_data`

**Script**: `app/data_to_server/main_raw_voice.py`

Usa `SessionSummaryVoice` (table3) como base con LEFT JOINs para preservar llamadas fallidas, bloqueadas y caídas. Sin
filtro de `CallDirection`. Almacena en `voice_raw_measurements`.

### Paso 4 — `load_datasource`

**Script**: `app/data_to_server/load_datasource.py`

Lee `extract_datasource.parquet`, enriquece con `Device` y `CZO` desde `sense_nacional_v0.xlsx`, normaliza teléfonos (
`+593XXXXXXXXX` → `0XXXXXXXXX`) y hace upsert en `datasource_phones`. Incluye limpieza post-upsert de duplicados.

### Paso 5 — `update_raw_mappings`

**Script**: `app/data_to_server/update_raw_mapping.py`

Mapea coordenadas de `mobile_raw_measurements` y `voice_raw_measurements` hacia regiones geográficas usando un índice
STRtree R-tree con geometrías cacheadas. Usa `geometry.covers(point)` para precisión en fronteras parroquiales. Almacena
en `location_mapping`.

### Paso 6 — `create_geo_views`

**Script**: `app/data_to_server/create_grafana_geo_views.py`

Crea o reemplaza `grafana_mobile_geo_view` y `grafana_voice_geo_view`. Cada vista une las tablas raw con
`location_mapping`, `geographic_regions` y `datasource_phones` añadiendo `Provincia`, `Cantón`, `Parroquia`,
`PhoneNumber`, `Device` y `CZO`.

| Vista                     | Fuente                    | Filtro aplicado en vista                          |
|---------------------------|---------------------------|---------------------------------------------------|
| `grafana_mobile_geo_view` | `mobile_raw_measurements` | `"SessionType" IN ('HTTP Post', 'HTTP Download')` |
| `grafana_voice_geo_view`  | `voice_raw_measurements`  | `"CallDirection" = 'MO'`                          |

### Paso 7 — `pipeline_validation`

Verifica conteos de tablas raw, regiones geográficas, catálogo, cobertura del mapeo espacial (umbral > 50%) y existencia
de vistas. Genera reporte en los logs de Airflow.

---

## Base de Datos

### Tablas

| Tabla                     | Creada por     | Contenido                                  |
|---------------------------|----------------|--------------------------------------------|
| `geographic_regions`      | Paso 1         | 1 081 parroquias con geometría GeoJSON     |
| `mobile_raw_measurements` | Paso 2         | Todos los SessionType sin filtro           |
| `voice_raw_measurements`  | Paso 3         | Todas las llamadas sin filtro de dirección |
| `datasource_phones`       | Paso 4         | `PhoneNumber ↔ IMSI ↔ IMEI ↔ Device ↔ CZO` |
| `location_mapping`        | Paso 5         | `measurement_id → region_id`               |
| `airflow_metadata`        | `airflow-init` | Metadatos internos de Airflow              |

### Decisiones de diseño

- **`ON CONFLICT DO NOTHING`**: inserciones idempotentes — re-ejecutar el DAG con los mismos Parquets no duplica
  registros.
- **`measurement_id` MD5**: clave determinista sobre campos de negocio, estable entre re-ejecuciones.
- **`TEXT` para strings**: evita `StringDataRightTruncation` de `VARCHAR(N)`.
- **`create_tables()` mínimo**: solo crea `geographic_regions`. Las tablas raw las crean `main_raw_mobile.py` y
  `main_raw_voice.py` vía `create_raw_tables()`.

---

## Vistas Disponibles

```sql
-- Datos móviles georeferenciados (HTTP Post + HTTP Download)
SELECT * FROM grafana_mobile_geo_view;

-- Datos de voz georeferenciados (llamadas MO)
SELECT * FROM grafana_voice_geo_view;
```

### Variables de Grafana compatibles

```sql
-- $SimOperator
SELECT DISTINCT "SimOperator"
FROM grafana_mobile_geo_view
WHERE "SimOperator" IS NOT NULL
ORDER BY "SimOperator";

-- $PhoneNumber (depende de $SimOperator)
SELECT DISTINCT "PhoneNumber"
FROM grafana_mobile_geo_view
WHERE "SimOperator" IN (${SimOperator})
  AND "PhoneNumber" IS NOT NULL
ORDER BY "PhoneNumber";

-- Filtro de tiempo:  "StartTime" >= $__timeFrom() AND "EndTime" <= $__timeTo()
```

---

## Administración

### Comandos útiles

```bash
# Estado de servicios
docker compose ps

# Logs en tiempo real
docker compose logs -f airflow-scheduler

# Contraseña de Airflow
docker compose logs airflow-webserver | grep "Password for user"

# Salud del API
curl http://localhost:8080/api/v2/monitor/health

# Reiniciar scheduler
docker compose restart airflow-scheduler
```

### Consultas de verificación

```sql
-- Conteo por tabla
SELECT 'mobile_raw_measurements' AS tabla, COUNT(*) FROM mobile_raw_measurements WHERE is_current = 1
UNION ALL
SELECT 'voice_raw_measurements',            COUNT(*) FROM voice_raw_measurements  WHERE is_current = 1
UNION ALL
SELECT 'geographic_regions',                COUNT(*) FROM geographic_regions       WHERE is_current = 1
UNION ALL
SELECT 'datasource_phones',                 COUNT(*) FROM datasource_phones
UNION ALL
SELECT 'location_mapping',                  COUNT(*) FROM location_mapping;

-- Cobertura del mapeo espacial
SELECT
    COUNT(DISTINCT m.measurement_id)                                              AS total,
    COUNT(DISTINCT lm.measurement_id)                                             AS mapeados,
    ROUND(COUNT(DISTINCT lm.measurement_id) * 100.0
          / NULLIF(COUNT(DISTINCT m.measurement_id), 0), 1)                       AS pct_cobertura
FROM mobile_raw_measurements m
LEFT JOIN location_mapping lm ON m.measurement_id = lm.measurement_id
WHERE m.is_current = 1;
```

---

## Carga Histórica Inicial

Procesar más de ~6 meses en una ejecución puede causar `SIGKILL -9` (OOM). Usar estrategia por trimestres:

1. En `samm_extract_data` (VM3), ajustar el rango de fechas.
2. Ejecutar extractores y transferir Parquets a VM2.
3. Disparar el DAG.
4. Repetir para el siguiente trimestre.

| Trimestre | `fecha_inicio` | `fecha_fin`      |
|-----------|----------------|------------------|
| Q1 2025   | 2025-01-01     | 2025-03-31       |
| Q2 2025   | 2025-04-01     | 2025-06-30       |
| Q3 2025   | 2025-07-01     | 2025-09-30       |
| Q4 2025   | 2025-10-01     | 2025-12-31       |
| 2026      | 2026-01-01     | *(fecha actual)* |

---

## Solución de Problemas

### `httpx.ConnectError` al ejecutar tareas

Verificar en `docker-compose.yaml`:

```yaml
AIRFLOW__CORE__EXECUTION_API_SERVER_URL: 'http://airflow-webserver:8080/execution/'
AIRFLOW__API_AUTH__JWT_SECRET: '${AIRFLOW__API_AUTH__JWT_SECRET}'
```

### `shapefile.shp: No such file or directory`

```bash
scp -r states/ root@XXX.XXX.XXX.51:/opt/samm_pipeline/app/data/
```

### `UndefinedTable: no existe la relación «geographic_regions»`

El paso 1 falló o no se ejecutó. Verificar presencia del shapefile y re-ejecutar el DAG desde el paso 1.

### `can't adapt type 'NAType'` o `DatatypeMismatch`

Parquets generados con versión desactualizada de `samm_extract_data`. Regenerarlos desde VM3 con la versión actual que
incluye `_cast_chunk()`.

### `SIGKILL -9` durante el procesamiento

Falta de RAM. Usar la [estrategia por trimestres](#carga-histórica-inicial).

### Porcentaje de mapeo espacial bajo (< 50%)

Verificar que las coordenadas estén dentro del bounding box de Ecuador (Lat: −5 a 2, Lon: −81 a −75).

### Contraseña de Airflow desconocida

```bash
docker compose logs airflow-webserver | grep "Password for user"
```