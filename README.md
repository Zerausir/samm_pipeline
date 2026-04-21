# samm_pipeline

[![Python 3.14](https://img.shields.io/badge/python-3.14-blue.svg)](https://www.python.org/)
[![Airflow 3.2.0](https://img.shields.io/badge/airflow-3.2.0-017CEE.svg)](https://airflow.apache.org/)
[![PostgreSQL 17](https://img.shields.io/badge/postgresql-17-336791.svg)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/docker-compose-2496ED.svg)](https://www.docker.com/)

Pipeline ETL completo para procesamiento automatizado de datos de telecomunicaciones móviles y de voz del sistema SAMM (
Sistema Automático de Medición de Redes Móviles), orquestado con Apache Airflow 3.2.0 y almacenado en PostgreSQL para
análisis en PowerBI y Grafana.

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
- [Pipeline ETL — 11 Pasos](#pipeline-etl--11-pasos)
- [Base de Datos](#base-de-datos)
- [Vistas Disponibles](#vistas-disponibles)
- [Administración](#administración)
- [Carga Histórica Inicial](#carga-histórica-inicial)
- [Solución de Problemas](#solución-de-problemas)

---

## Descripción

Este pipeline recibe archivos Parquet generados por `samm_extract_data`, los transforma, enriquece con datos
geoespaciales, los almacena en PostgreSQL y crea vistas optimizadas para su consumo en PowerBI y Grafana. Corre
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
                                    Airflow DAG    PostgreSQL 17    PowerBI / Grafana
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

### Servicios Docker — VM1

| Servicio                | Imagen                 | Puerto | Función                       |
|-------------------------|------------------------|--------|-------------------------------|
| `airflow-webserver`     | `custom-airflow:3.2.0` | 8080   | API server + UI (Airflow 3.x) |
| `airflow-scheduler`     | `custom-airflow:3.2.0` | —      | Orquestador del DAG           |
| `airflow-triggerer`     | `custom-airflow:3.2.0` | —      | Tareas diferibles             |
| `airflow-dag-processor` | `custom-airflow:3.2.0` | —      | Parser de DAGs                |
| `airflow-init`          | `custom-airflow:3.2.0` | —      | Inicialización única          |
| `data-processor`        | `python:3.14-slim`     | —      | Contenedor auxiliar           |

> PostgreSQL **no** corre en Docker — está gestionado como servicio bare-metal en VM2.

### DAG: `superset_etl_pipeline`

Pipeline **completamente secuencial** — cada paso depende del anterior:

```
process_mobile_data        (paso  1) — ETL móvil clean → mobile_measurements
        │
process_voice_data         (paso  2) — ETL voz clean → voice_measurements
        │
process_raw_mobile_data    (paso  3) — ETL móvil raw → mobile_raw_measurements
        │
process_raw_voice_data     (paso  4) — ETL voz raw → voice_raw_measurements
        │
load_datasource            (paso  5) — Catálogo → datasource_phones
        │
update_all_mappings        (paso  6) — Mapeo espacial (tablas clean)
        │
update_raw_mappings        (paso  7) — Mapeo espacial (tablas raw)
        │
create_mobile_views        (paso  8) — Vista PowerBI móvil
        │
create_voice_views         (paso  9) — Vista PowerBI voz
        │
create_grafana_geo_views   (paso 10) — Vistas Grafana georeferenciadas
        │
pipeline_validation        (paso 11) — Validación final del pipeline
```

**Horario**: diariamente a las 09:00 y 14:00 (zona horaria `America/Guayaquil`).

---

## Requisitos

### Software

- Docker Desktop (Windows/Linux) o Docker Engine (Linux)
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
│   │   └── superset_etl_dag.py              # DAG principal — 11 pasos secuenciales
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
│   │   ├── main.py                          # ETL móvil clean → mobile_measurements
│   │   ├── main_voice.py                    # ETL voz clean → voice_measurements
│   │   ├── main_raw_mobile.py               # ETL móvil raw → mobile_raw_measurements
│   │   ├── main_raw_voice.py                # ETL voz raw → voice_raw_measurements
│   │   ├── load_datasource.py               # Catálogo → datasource_phones
│   │   ├── update_mapping.py                # Mapeo espacial (tablas clean)
│   │   ├── update_raw_mapping.py            # Mapeo espacial (tablas raw)
│   │   ├── create_dashboard_view.py         # Vista PowerBI — datos móviles
│   │   ├── create_dashboard_view_voice.py   # Vista PowerBI — datos de voz
│   │   └── create_grafana_geo_views.py      # Vistas Grafana georeferenciadas
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

Crear `.env` en la raíz basado en `.env.example`:

```bash
cp .env.example .env
```

```env
# ─── PostgreSQL (VM2 — SAMMPBI1) ──────────────────────────────────────────
POSTGRES_HOST=XXX.XXX.XXX.50
POSTGRES_PORT=5432
POSTGRES_DB=samm_db
POSTGRES_USER=samm_user
POSTGRES_PASSWORD=<contraseña>

# ─── Airflow ──────────────────────────────────────────────────────────────
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=<contraseña_ui>

# JWT para comunicación scheduler ↔ api-server (Airflow 3.x — obligatorio)
# Generar con: $bytes = New-Object byte[] 32; [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes); [System.Convert]::ToBase64String($bytes)
AIRFLOW__API_AUTH__JWT_SECRET=<jwt_secret_base64>
```

> 🔒 El archivo `.env` está en `.gitignore`. Nunca lo incluyas en un commit.

---

## Instalación

### 1. Clonar el repositorio

```bash
git clone https://github.com/Zerausir/samm_pipeline.git
cd samm_pipeline
```

### 2. Configurar variables de entorno

```bash
cp .env.example .env
# Editar .env con los valores correctos
```

### 3. Crear directorios necesarios

```bash
mkdir -p airflow/dags airflow/logs airflow/plugins app/data/states
```

### 4. Construir imagen personalizada de Airflow

La imagen extiende `apache/airflow:slim-3.2.0-python3.14` con dependencias geoespaciales y de procesamiento de datos:

```bash
docker build -f Dockerfile.airflow -t custom-airflow:3.2.0 .
```

Dependencias Python incluidas: `numpy==2.4.4`, `asyncpg==0.31.0`, `psycopg2-binary==2.9.11`, `geopandas==1.1.3`,
`pyodbc==5.3.0`, `environs==15.0.1`, `openpyxl==3.1.5`, `pykml==0.2.0`, `sqlalchemy==2.0.49`, `pyarrow==23.0.1`.

### 5. Inicializar y arrancar servicios

```bash
# Inicializar base de datos de Airflow (ejecutar una sola vez)
docker compose up airflow-init

# Arrancar todos los servicios en segundo plano
docker compose up -d

# Verificar que todos los servicios estén healthy
docker compose ps
```

Todos los servicios deben aparecer como `healthy` antes de disparar el DAG:

```
airflow-webserver     healthy    0.0.0.0:8080->8080/tcp
airflow-scheduler     healthy
airflow-triggerer     healthy
airflow-dag-processor healthy
```

### 6. Copiar archivos de datos

Después de ejecutar `samm_extract_data` en VM3, copiar los Parquets al directorio `app/data/` de VM1:

```bash
scp *.parquet usuario@XXX.XXX.XXX.51:/ruta/samm_pipeline/app/data/
```

### 7. Acceder a Airflow

Abrir [http://localhost:8080](http://localhost:8080) con las credenciales configuradas en `.env`.

---

## Archivos de Datos Requeridos

Todos los archivos deben estar presentes en `app/data/` antes de ejecutar el DAG:

| Archivo                                           | Origen                                             |
|---------------------------------------------------|----------------------------------------------------|
| `extract_datos_table1.parquet`                    | `samm_extract_data` → `extract_data_datos.py`      |
| `extract_datos_table2.parquet`                    | `samm_extract_data` → `extract_data_datos.py`      |
| `extract_voz_table1.parquet`                      | `samm_extract_data` → `extract_data_voz.py`        |
| `extract_voz_table3.parquet`                      | `samm_extract_data` → `extract_data_voz.py`        |
| `extract_voz_table4.parquet`                      | `samm_extract_data` → `extract_data_voz.py`        |
| `extract_datasource.parquet`                      | `samm_extract_data` → `extract_data_datasource.py` |
| `sense_nacional_v0.xlsx`                          | Provisto manualmente                               |
| `states/shapefile.shp` (+ `.shx`, `.dbf`, `.prj`) | Provisto manualmente                               |

```bash
# Verificar presencia de todos los archivos
ls -la app/data/extract_*.parquet
ls -la app/data/sense_nacional_v0.xlsx
ls -la app/data/states/shapefile.*
```

---

## Pipeline ETL — 11 Pasos

### Paso 1 — `process_mobile_data`

**Script**: `app/data_to_server/main.py`

Carga `extract_datos_table1.parquet` (SessionSummary) y `extract_datos_table2.parquet` (SessionSummaryData). Filtra
sesiones `HTTP Download` y `HTTP Post`, elimina duplicados, realiza merge chunked (10 000 filas/chunk), calcula
`ThroughputMbps`, filtra registros sin coordenadas válidas ni throughput y almacena en `mobile_measurements` usando
`execute_batch` + `ON CONFLICT DO NOTHING`.

### Paso 2 — `process_voice_data`

**Script**: `app/data_to_server/main_voice.py`

Carga los 3 archivos Parquet de voz. Usa `SessionSummaryVoice` (table3) como base con LEFT JOINs para preservar llamadas
fallidas, bloqueadas y caídas. Filtra sesiones `Voice MO`, realiza merge chunked y almacena en `voice_measurements`.

### Paso 3 — `process_raw_mobile_data`

**Script**: `app/data_to_server/main_raw_mobile.py`

Mismos Parquets que el paso 1 pero **sin** `dropna` de coordenadas ni throughput — sesiones fallidas son datos
regulatorios válidos. Almacena todos los `SessionType` en `mobile_raw_measurements` para consumo en Grafana.

### Paso 4 — `process_raw_voice_data`

**Script**: `app/data_to_server/main_raw_voice.py`

Mismos Parquets que el paso 2 pero **sin** filtro de `CallDirection` — la vista Grafana filtra `CallDirection='MO'`.
Almacena en `voice_raw_measurements`.

### Paso 5 — `load_datasource`

**Script**: `app/data_to_server/load_datasource.py`

Lee `extract_datasource.parquet` (PhoneNumber ↔ IMSI ↔ IMEI), enriquece con `Device` y `CZO` desde
`sense_nacional_v0.xlsx`, normaliza formato de teléfonos (`+593XXXXXXXXX` → `0XXXXXXXXX`) y hace upsert en
`datasource_phones`.

### Paso 6 — `update_all_mappings`

**Script**: `app/data_to_server/update_mapping.py`

Mapea coordenadas de `mobile_measurements` y `voice_measurements` (tablas clean) a regiones geográficas del shapefile
mediante un índice STRtree R-tree. Usa `geometry.covers(point)` para precisión en puntos de frontera. Almacena en
`location_mapping`.

### Paso 7 — `update_raw_mappings`

**Script**: `app/data_to_server/update_raw_mapping.py`

Mismo proceso que el paso 6 pero sobre `mobile_raw_measurements` y `voice_raw_measurements`. Comparte la tabla
`location_mapping`.

### Paso 8 — `create_mobile_views`

**Script**: `app/data_to_server/create_dashboard_view.py`

Crea o reemplaza la vista `data_dashboard_view_visualization` para consumo en PowerBI (DirectQuery).

### Paso 9 — `create_voice_views`

**Script**: `app/data_to_server/create_dashboard_view_voice.py`

Crea o reemplaza la vista `voice_dashboard_view_visualization` para consumo en PowerBI.

### Paso 10 — `create_grafana_geo_views`

**Script**: `app/data_to_server/create_grafana_geo_views.py`

Crea las vistas `grafana_mobile_geo_view` y `grafana_voice_geo_view`. Cada vista une las tablas raw con
`location_mapping`, `geographic_regions` y `datasource_phones` para exponer `Provincia`, `Cantón`, `Parroquia`,
`PhoneNumber`, `Device` y `CZO` directamente accesibles desde Grafana.

### Paso 11 — `pipeline_validation`

Valida integridad de datos en todas las tablas y vistas, porcentaje de cobertura del mapeo espacial y disponibilidad de
todas las vistas. Genera un reporte consolidado en los logs de Airflow.

---

## Base de Datos

### Tablas principales

| Tabla                     | Contenido                                                                             | Destino |
|---------------------------|---------------------------------------------------------------------------------------|---------|
| `mobile_measurements`     | Sesiones HTTP Download/Post con coordenadas y ThroughputMbps válidos                  | PowerBI |
| `voice_measurements`      | Llamadas Voice MO con coordenadas válidas                                             | PowerBI |
| `mobile_raw_measurements` | Todos los SessionType sin filtro de coordenadas                                       | Grafana |
| `voice_raw_measurements`  | Todas las llamadas (MO + MT) sin filtro de coordenadas                                | Grafana |
| `geographic_regions`      | 1 081 parroquias del Ecuador (shapefile) — se inserta una sola vez                    | Ambos   |
| `location_mapping`        | `measurement_id` → `region_id` con clave `(measurement_id, region_id, location_type)` | Ambos   |
| `datasource_phones`       | `PhoneNumber ↔ IMSI ↔ IMEI ↔ Device ↔ CZO` — clave MD5 `phone_id`                     | Ambos   |
| `airflow_metadata`        | Metadatos internos de Airflow                                                         | Airflow |

### Decisiones de diseño

- **`ON CONFLICT DO NOTHING`**: todas las inserciones son idempotentes. Re-ejecutar el DAG con los mismos Parquets no
  duplica registros.
- **`measurement_id` MD5**: clave primaria determinista calculada sobre campos de negocio — estable entre
  re-ejecuciones.
- **`TEXT` para columnas string**: evita errores `StringDataRightTruncation` de `VARCHAR(N)` con valores más largos que
  el tamaño muestreado.
- **`execute_batch` + chunks de 5 000**: reduce los round-trips a PostgreSQL de O(4N) a O(N/5000).

---

## Vistas Disponibles

### PowerBI

```sql
-- Datos móviles con información geográfica completa
SELECT * FROM data_dashboard_view_visualization;

-- Datos de voz con información geográfica completa
SELECT * FROM voice_dashboard_view_visualization;
```

### Grafana

```sql
-- HTTP Post + HTTP Download georeferenciados — con Provincia, Cantón, Parroquia, PhoneNumber, Device, CZO
SELECT * FROM grafana_mobile_geo_view;

-- Llamadas MO georeferenciadas — con Provincia, Cantón, Parroquia, PhoneNumber, Device, CZO
SELECT * FROM grafana_voice_geo_view;
```

#### Columnas enriquecidas en vistas Grafana

| Columna         | Fuente                          | Descripción                   |
|-----------------|---------------------------------|-------------------------------|
| `"Provincia"`   | `geographic_regions.dpa_despro` | Nombre de la provincia        |
| `"Cantón"`      | `geographic_regions.dpa_descan` | Nombre del cantón             |
| `"Parroquia"`   | `geographic_regions.dpa_despar` | Nombre de la parroquia        |
| `"PhoneNumber"` | `datasource_phones`             | Número de teléfono del equipo |
| `"Device"`      | `datasource_phones`             | Modelo del dispositivo        |
| `"CZO"`         | `datasource_phones`             | Zona de control operativa     |

#### Variables de Grafana compatibles

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

-- Filtro de tiempo estándar
-- "StartTime" >= $__timeFrom() AND "EndTime" <= $__timeTo()
```

---

## Administración

### Comandos útiles

```bash
# Ver estado de todos los servicios
docker compose ps

# Ver logs en tiempo real del scheduler
docker compose logs -f airflow-scheduler

# Ver logs de una tarea específica del DAG
# Los logs están en airflow/logs/dag_id=superset_etl_pipeline/

# Disparar el DAG manualmente desde la UI
# Airflow UI → DAGs → superset_etl_pipeline → Trigger DAG

# Verificar salud de la API de Airflow 3.x
curl http://localhost:8080/api/v2/monitor/health

# Reiniciar solo el scheduler sin bajar todo el stack
docker compose restart airflow-scheduler

# Ver uso de recursos en tiempo real
docker stats
```

### Consultas de verificación en PostgreSQL

```sql
-- Conteo por tabla
SELECT 'mobile_measurements'     AS tabla, COUNT(*) FROM mobile_measurements WHERE is_current = 1
UNION ALL
SELECT 'voice_measurements',              COUNT(*) FROM voice_measurements WHERE is_current = 1
UNION ALL
SELECT 'mobile_raw_measurements',         COUNT(*) FROM mobile_raw_measurements WHERE is_current = 1
UNION ALL
SELECT 'voice_raw_measurements',          COUNT(*) FROM voice_raw_measurements WHERE is_current = 1
UNION ALL
SELECT 'location_mapping',                COUNT(*) FROM location_mapping
UNION ALL
SELECT 'datasource_phones',               COUNT(*) FROM datasource_phones;

-- Cobertura del mapeo espacial móvil
SELECT
    COUNT(DISTINCT m.measurement_id)                                             AS total_mediciones,
    COUNT(DISTINCT lm.measurement_id)                                            AS con_mapeo,
    ROUND(COUNT(DISTINCT lm.measurement_id) * 100.0
          / NULLIF(COUNT(DISTINCT m.measurement_id), 0), 1)                      AS pct_cobertura
FROM mobile_measurements m
LEFT JOIN location_mapping lm ON m.measurement_id = lm.measurement_id
WHERE m.is_current = 1;
```

---

## Carga Histórica Inicial

Procesar períodos superiores a ~6 meses en una sola ejecución puede causar `SIGKILL -9` (OOM) ya que el merge en memoria
requiere mantener múltiples DataFrames grandes simultáneamente.

### Estrategia por trimestres

1. En `samm_extract_data`, ajustar el rango de fechas:
   ```python
   fecha_inicio = datetime.datetime(2025, 1, 1)
   fecha_fin    = datetime.datetime(2025, 3, 31)
   ```
2. Ejecutar los extractores en VM3 y copiar los Parquets a VM1.
3. Disparar el DAG en Airflow.
4. Repetir para el siguiente trimestre.

Los datos se acumulan correctamente en cada ejecución gracias a `ON CONFLICT DO NOTHING`.

| Trimestre | `fecha_inicio` | `fecha_fin`      |
|-----------|----------------|------------------|
| Q1 2025   | 2025-01-01     | 2025-03-31       |
| Q2 2025   | 2025-04-01     | 2025-06-30       |
| Q3 2025   | 2025-07-01     | 2025-09-30       |
| Q4 2025   | 2025-10-01     | 2025-12-31       |
| 2026      | 2026-01-01     | *(fecha actual)* |

---

## Solución de Problemas

### `httpx.ConnectError: [Errno 111] Connection refused` al ejecutar tareas

El Task SDK de Airflow 3.x requiere comunicación HTTP con el api-server. Verificar en `docker-compose.yaml`:

```yaml
AIRFLOW__CORE__EXECUTION_API_SERVER_URL: 'http://airflow-webserver:8080/execution/'
AIRFLOW__API_AUTH__JWT_SECRET: '${AIRFLOW__API_AUTH__JWT_SECRET}'
```

Ambas variables son **obligatorias** en Airflow 3.x.

### `Invalid value for dtype 'str'` al procesar Parquets

Los Parquets deben generarse con `samm_extract_data` versión actualizada que incluye `_cast_chunk()`. Esta función
normaliza columnas enteras a `float64` e `IpAddress` a `str` antes de serializar a Arrow, garantizando compatibilidad
con pandas 3.x + pyarrow backend.

Si los Parquets fueron generados con una versión anterior, regenerarlos desde VM3 con el extractor actualizado.

### `DatatypeMismatch` en columnas booleanas al insertar en PostgreSQL

Las columnas `MultiRab`, `CarrierAggregation` y `CarrierAggregationUplink` están definidas como `BOOLEAN` en PostgreSQL.
No deben incluirse en la lista de conversión `int_columns → float64` de `_process_table2_session_summary_data`.

### `SIGKILL -9` durante el procesamiento

Indica falta de RAM — el merge en memoria excedió la RAM disponible del host. Usar
la [estrategia por trimestres](#estrategia-por-trimestres).

### Tarea con estado `state mismatch` en la UI de Airflow

Ocurre cuando el ejecutor reporta fallo pero la tarea no llegó a iniciar. Causas comunes:

1. `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` no configurado → el worker no puede conectar con el api-server.
2. `AIRFLOW__API_AUTH__JWT_SECRET` no configurado o diferente entre servicios → autenticación JWT falla.

Verificar ambas variables en `.env` y `docker-compose.yaml`, luego `docker compose down && docker compose up -d`.

### Logs de tareas vacíos

Si el log de una tarea aparece vacío, la tarea falló antes de iniciar. Revisar los logs del scheduler:

```bash
docker compose logs airflow-scheduler --tail=100 | grep -E "failed|error|ConnectError"
```

### Porcentaje de mapeo espacial bajo (< 80%)

Indica que muchas coordenadas caen fuera del shapefile. Verificar:

1. Que el shapefile en `app/data/states/` corresponde a Ecuador.
2. Que las coordenadas en los Parquets están en el rango correcto (latitud −5 a 2, longitud −81 a −75).
3. El método `geometry.covers(point)` (no `point.within(geometry)`) está en uso en `spatial_mapper.py` para correcta
   detección de puntos en frontera.