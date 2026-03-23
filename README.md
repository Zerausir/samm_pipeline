# sma_superset

[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/)
[![Airflow 2.7.3](https://img.shields.io/badge/airflow-2.7.3-017CEE.svg)](https://airflow.apache.org/)
[![PostgreSQL 17](https://img.shields.io/badge/postgresql-17-336791.svg)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/docker-compose-2496ED.svg)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Pipeline ETL completo para procesamiento de datos de telecomunicaciones móviles y de voz, orquestado con Apache Airflow
y almacenado en PostgreSQL para análisis en PowerBI y Grafana.

---

## Tabla de Contenidos

- [Descripción](#descripción)
- [Arquitectura](#arquitectura)
- [Requisitos](#requisitos)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Configuración](#configuración)
- [Instalación](#instalación)
- [Pipeline ETL](#pipeline-etl)
- [Base de Datos](#base-de-datos)
- [Vistas Disponibles](#vistas-disponibles)
- [Administración](#administración)
- [Carga Histórica Inicial](#carga-histórica-inicial)
- [Solución de Problemas](#solución-de-problemas)

---

## Descripción

Este pipeline recibe archivos Parquet generados por [
`samm_extract_data`](https://github.com/Zerausir/samm_extract_data), los procesa, los almacena en PostgreSQL y crea
vistas optimizadas para su consumo en PowerBI y Grafana. El sistema corre completamente dockerizado con Apache Airflow
como orquestador.

### Flujo completo de datos

```
[SQL Server] ──► [samm_extract_data] ──► [Archivos Parquet]
                    (máquina AD)                  │
                                                  ▼
                                           [sma_superset]
                                                  │
                             ┌────────────────────┼─────────────────────┐
                             ▼                    ▼                     ▼
                        Airflow DAG         PostgreSQL             PowerBI / Grafana
                        (orquesta)    (almacena + vistas)          (visualiza)
```

---

## Arquitectura

### Servicios Docker

| Servicio            | Imagen                 | Puerto | Función                              |
|---------------------|------------------------|--------|--------------------------------------|
| `postgres`          | `postgres:17-alpine`   | 5432   | Base de datos principal              |
| `airflow-webserver` | `custom-airflow:2.7.3` | 8080   | UI de monitoreo                      |
| `airflow-scheduler` | `custom-airflow:2.7.3` | —      | Orquestador del DAG                  |
| `airflow-init`      | `custom-airflow:2.7.3` | —      | Inicialización única del ambiente    |
| `data-processor`    | `python:3.11`          | —      | Contenedor auxiliar de procesamiento |

### DAG: `superset_etl_pipeline`

El pipeline es **completamente secuencial** — cada paso depende del anterior:

```
process_mobile_data          (paso 1)
        │
process_voice_data           (paso 2)
        │
process_raw_mobile_data      (paso 3)
        │
process_raw_voice_data       (paso 4)
        │
load_datasource              (paso 5)
        │
update_all_mappings          (paso 6)
        │
update_raw_mappings          (paso 7)
        │
create_mobile_views          (paso 8)
        │
create_voice_views           (paso 9)
        │
create_grafana_geo_views     (paso 10)
        │
pipeline_validation          (paso 11)
```

**Horario**: diariamente a las 09:00 y 14:00 (zona horaria `America/New_York`).

---

## Requisitos

### Software

- Docker Desktop
- Docker Compose
- Python 3.11+ (solo para el paso de extracción en máquina AD)

### Hardware mínimo

- 8 GB de RAM para el host de Docker
- 20 GB de espacio en disco

> ⚠️ Los contenedores de Airflow no tienen límites de memoria configurados — usan toda la RAM disponible del host. Para
> volúmenes de datos grandes (>6 meses de historia), ver [Carga Histórica Inicial](#carga-histórica-inicial).

---

## Estructura del Proyecto

```
sma_superset/
├── airflow/
│   ├── dags/
│   │   └── superset_etl_dag.py              # DAG principal — 11 pasos secuenciales
│   ├── logs/                                # Logs de ejecución (auto-generado)
│   └── plugins/
│       ├── custom_hooks.py                  # PostgreSQLCustomHook
│       └── custom_operators.py              # WindowsServiceOperator
├── app/
│   ├── data/                                # Archivos Parquet y estáticos (NO en git)
│   │   ├── extract_datos_table1.parquet     # ← generado por samm_extract_data
│   │   ├── extract_datos_table2.parquet     # ← generado por samm_extract_data
│   │   ├── extract_voz_table1.parquet       # ← generado por samm_extract_data
│   │   ├── extract_voz_table3.parquet       # ← generado por samm_extract_data
│   │   ├── extract_voz_table4.parquet       # ← generado por samm_extract_data
│   │   ├── extract_datasource.parquet       # ← generado por samm_extract_data
│   │   ├── sense_nacional_v0.xlsx           # ← provisto manualmente
│   │   └── states/                          # Shapefile de Ecuador
│   │       ├── shapefile.shp
│   │       ├── shapefile.shx
│   │       ├── shapefile.dbf
│   │       └── shapefile.prj
│   ├── data_to_server/
│   │   ├── main.py                          # ETL datos móviles → mobile_measurements
│   │   ├── main_voice.py                    # ETL datos de voz → voice_measurements
│   │   ├── main_raw_mobile.py               # ETL raw móvil → mobile_raw_measurements
│   │   ├── main_raw_voice.py                # ETL raw voz → voice_raw_measurements
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
│       └── spatial_mapper.py               # Mapeador espacial con STRtree
├── diagrama_arquitectura/                   # Diagramas HTML interactivos
├── excel_para_fabric/
│   └── transfer.py                          # Exportación a Excel (auxiliar)
├── nginx/
│   └── nginx.conf                           # Reverse proxy HTTPS (opcional)
├── postgres-conf/
│   └── postgresql.conf                      # Tuning PostgreSQL
├── scripts/
│   ├── start_airflow.bat                    # Inicio local (Windows)
│   ├── start_postgres.bat                   # Inicio local (Windows)
│   └── start_superset.bat                   # Inicio local (Windows)
├── superset/
│   ├── __init__.py
│   └── superset_config.py                   # Config Apache Superset (opcional)
├── Dockerfile.airflow                        # Imagen personalizada de Airflow
├── docker-compose.yaml
├── requirements.txt
├── .env                                     # Variables de entorno (NO en git)
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
# PostgreSQL
POSTGRES_HOST=172.x.x.x
POSTGRES_PORT=5432
POSTGRES_DB=nombre_base_datos
POSTGRES_USER=usuario
POSTGRES_PASSWORD=contraseña

# Airflow
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=contraseña_admin

# Rutas de archivos estáticos (dentro del contenedor)
sense_file=/app/data/sense_nacional_v0.xlsx
geojson_route=/app/data/states

# Superset (opcional)
SUPERSET_SECRET_KEY=clave_secreta
MAPBOX_API_KEY=tu_mapbox_api_key
```

> 🔒 El archivo `.env` está en `.gitignore`. Nunca lo incluyas en un commit.

### Archivos de datos requeridos

Antes de ejecutar el pipeline, los siguientes archivos deben estar presentes en `app/data/`:

| Archivo                                    | Origen                                             |
|--------------------------------------------|----------------------------------------------------|
| `extract_datos_table1.parquet`             | `samm_extract_data` → `extract_data_datos.py`      |
| `extract_datos_table2.parquet`             | `samm_extract_data` → `extract_data_datos.py`      |
| `extract_voz_table1.parquet`               | `samm_extract_data` → `extract_data_voz.py`        |
| `extract_voz_table3.parquet`               | `samm_extract_data` → `extract_data_voz.py`        |
| `extract_voz_table4.parquet`               | `samm_extract_data` → `extract_data_voz.py`        |
| `extract_datasource.parquet`               | `samm_extract_data` → `extract_data_datasource.py` |
| `sense_nacional_v0.xlsx`                   | Provisto manualmente                               |
| `states/shapefile.shp` (+.shx, .dbf, .prj) | Provisto manualmente                               |

```bash
# Verificar presencia de archivos
ls -la app/data/extract_*.parquet
ls -la app/data/sense_nacional_v0.xlsx
ls -la app/data/states/
```

---

## Instalación

### 1. Clonar el repositorio

```bash
git clone https://github.com/Zerausir/sma_superset.git
cd sma_superset
```

### 2. Configurar variables de entorno

```bash
cp .env.example .env
# Editar .env con los valores correctos
```

### 3. Crear directorios necesarios

```bash
mkdir -p airflow/dags airflow/logs airflow/plugins app/data
```

### 4. Construir imagen personalizada de Airflow

La imagen incluye dependencias adicionales: `geopandas`, `pyarrow`, `psycopg2`, `shapely`, `libspatialindex-dev`.

```bash
docker build -f Dockerfile.airflow -t custom-airflow:2.7.3 .
```

### 5. Inicializar y arrancar servicios

```bash
# Inicializar base de datos de Airflow
docker-compose up airflow-init

# Arrancar todos los servicios
docker-compose up -d

# Verificar estado
docker-compose ps
```

### 6. Copiar archivos de datos

```bash
# Desde la máquina con acceso AD (después de ejecutar samm_extract_data)
scp /ruta/local/*.parquet usuario@servidor:/ruta/sma_superset/app/data/
```

### 7. Acceder a Airflow

Abrir [http://localhost:8080](http://localhost:8080) con las credenciales configuradas en `.env`.

---

## Pipeline ETL

### Paso 1 — `process_mobile_data`

**Script**: `app/data_to_server/main.py`

Carga `extract_datos_table1.parquet` y `extract_datos_table2.parquet`. Filtra sesiones `HTTP Download` y `HTTP Post`,
elimina duplicados, realiza merge chunked entre tablas, calcula `ThroughputMbps`, filtra coordenadas inválidas y
almacena en `mobile_measurements` usando `execute_batch` + `ON CONFLICT DO NOTHING`.

### Paso 2 — `process_voice_data`

**Script**: `app/data_to_server/main_voice.py`

Carga los 3 archivos Parquet de voz. Filtra sesiones `Voice MO`, realiza merge chunked con `SessionSummaryVoice` como
tabla intermedia, filtra coordenadas inválidas y almacena en `voice_measurements`. Incluye `CallDroppedDateTime` y
`CallBlockedDateTime` para métricas regulatorias completas.

### Paso 3 — `process_raw_mobile_data`

**Script**: `app/data_to_server/main_raw_mobile.py`

Mismo proceso que el paso 1, **sin** filtro de `SessionType` y **sin** `dropna` de coordenadas ni `ThroughputMbps`.
Almacena en `mobile_raw_measurements`. Los filtros se aplican en la vista Grafana, no en la tabla.

### Paso 4 — `process_raw_voice_data`

**Script**: `app/data_to_server/main_raw_voice.py`

Usa `SessionSummaryVoice` (table3) como base del merge para capturar **todas** las llamadas — establecidas, fallidas,
bloqueadas y caídas. Sin `dropna`. Almacena en `voice_raw_measurements`.

### Paso 5 — `load_datasource`

**Script**: `app/data_to_server/load_datasource.py`

Lee `extract_datasource.parquet` (PhoneNumber, IMSI, IMEI) y enriquece con Device y CZO desde `sense_nacional_v0.xlsx`.
Realiza upsert en `datasource_phones` e incluye limpieza post-upsert: normalización de `+593XXXXXXXXX → 0XXXXXXXXX`,
eliminación de duplicados por IMEI y eliminación de registros sin Device/CZO.

### Paso 6 — `update_all_mappings`

**Script**: `app/data_to_server/update_mapping.py`

Mapea coordenadas de `mobile_measurements` y `voice_measurements` a regiones geográficas usando un índice espacial *
*STRtree** (R-tree de Shapely). Complejidad O(n·log(r)) vs O(n·r) del algoritmo original. Las geometrías se cargan desde
`geographic_regions` y se cachean en memoria para evitar doble consulta a la base de datos.

### Paso 7 — `update_raw_mappings`

**Script**: `app/data_to_server/update_raw_mapping.py`

Mismo proceso que el paso 6 pero sobre `mobile_raw_measurements` y `voice_raw_measurements`. Reutiliza el cache de
geometrías si ya fue cargado en el paso 6.

### Paso 8 — `create_mobile_views`

**Script**: `app/data_to_server/create_dashboard_view.py`

Recrea la vista `data_dashboard_view_visualization` con todas las columnas de `mobile_measurements` más información
geográfica (provincia, cantón, parroquia). Se elimina y recrea en cada ejecución para reflejar cambios de schema.

### Paso 9 — `create_voice_views`

**Script**: `app/data_to_server/create_dashboard_view_voice.py`

Ídem para datos de voz: vista `voice_dashboard_view_visualization`.

### Paso 10 — `create_grafana_geo_views`

**Script**: `app/data_to_server/create_grafana_geo_views.py`

Crea vistas georeferenciadas para los dashboards de Grafana. Los filtros de `SessionType` y `CallDirection` se aplican
en la definición de la vista para máxima extensibilidad.

### Paso 11 — `pipeline_validation`

Valida integridad completa: conteo de registros en tablas clean y raw, porcentaje de mapeo geográfico (umbral mínimo
50%), existencia de todas las vistas, registros en `datasource_phones`. Genera reporte de estado.

---

## Base de Datos

### Tablas principales

#### `mobile_measurements` / `voice_measurements`

Datos analíticos limpios para PowerBI. Schema dinámico — columnas agregadas automáticamente según el Parquet de entrada.

| Columna                | Tipo                               | Descripción                             |
|------------------------|------------------------------------|-----------------------------------------|
| `measurement_id`       | `VARCHAR` PK                       | Hash MD5 determinístico de campos clave |
| `valid_from`           | `TIMESTAMP`                        | Timestamp de inserción                  |
| `is_current`           | `INTEGER`                          | Flag de registro activo (1)             |
| `batch_id`             | `VARCHAR`                          | ID del batch de inserción               |
| *(columnas dinámicas)* | `TEXT` / `DOUBLE PRECISION` / etc. | Todas las columnas del Parquet          |

#### `mobile_raw_measurements` / `voice_raw_measurements`

Datos regulatorios completos para Grafana. Misma estructura que las tablas clean pero sin `dropna` de coordenadas ni
ThroughputMbps — sesiones fallidas son datos regulatorios válidos.

#### `geographic_regions`

Regiones geográficas del shapefile de Ecuador (1 081 parroquias). Se inserta una sola vez al inicio.

#### `location_mapping`

Mapa entre `measurement_id` y `region_id`. Clave compuesta `(measurement_id, region_id, location_type)`.

#### `datasource_phones`

Catálogo de dispositivos: `PhoneNumber ↔ IMSI ↔ IMEI ↔ Device ↔ CZO`. Clave `phone_id` (MD5 de PhoneNumber + IMEI).

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
-- HTTP Post + HTTP Download (regulatorio) — con Provincia, Cantón, Parroquia, PhoneNumber, Device, CZO
SELECT * FROM grafana_mobile_geo_view;

-- Llamadas MO (regulatorio) — con Provincia, Cantón, Parroquia, PhoneNumber, Device, CZO
SELECT * FROM grafana_voice_geo_view;
```

#### Columnas adicionales en vistas Grafana

| Columna         | Fuente                          | Descripción                   |
|-----------------|---------------------------------|-------------------------------|
| `"Provincia"`   | `geographic_regions.dpa_despro` | Nombre de la provincia        |
| `"Cantón"`      | `geographic_regions.dpa_descan` | Nombre del cantón             |
| `"Parroquia"`   | `geographic_regions.dpa_despar` | Nombre de la parroquia        |
| `"PhoneNumber"` | `datasource_phones`             | Número de teléfono del equipo |
| `"Device"`      | `datasource_phones`             | Modelo del dispositivo        |
| `"CZO"`         | `datasource_phones`             | Zona de control operativa     |

#### Variables Grafana compatibles

```sql
-- $SimOperator
SELECT DISTINCT "SimOperator" FROM grafana_mobile_geo_view WHERE "SimOperator" IS NOT NULL ORDER BY "SimOperator";

-- $PhoneNumber
SELECT DISTINCT "PhoneNumber" FROM grafana_mobile_geo_view
WHERE "SimOperator" IN (${SimOperator}) AND "PhoneNumber" IS NOT NULL ORDER BY "PhoneNumber";

-- Filtro de tiempo: "StartTime" >= $__timeFrom() AND "EndTime" <= $__timeTo()
```

---

## Administración

### Comandos útiles

```bash
# Ver estado de servicios
docker-compose ps

# Ver logs en tiempo real
docker-compose logs -f airflow-scheduler

# Ejecutar DAG manualmente
docker-compose exec airflow-webserver airflow dags trigger superset_etl_pipeline

# Consultar registros en PostgreSQL
docker-compose exec postgres psql -U airflow -d airflow -c \
  "SELECT COUNT(*) FROM mobile_measurements WHERE is_current = 1;"

# Verificar mapeo geográfico
docker-compose exec postgres psql -U airflow -d airflow -c \
  "SELECT COUNT(*) FROM location_mapping;"

# Verificar catálogo de dispositivos
docker-compose exec postgres psql -U airflow -d airflow -c \
  "SELECT COUNT(*) FROM datasource_phones WHERE \"Device\" IS NOT NULL;"

# Verificar uso de recursos
docker stats
```

---

## Carga Histórica Inicial

Procesar más de ~6 meses de datos en una sola ejecución puede causar `SIGKILL -9` (OOM del sistema operativo) dado que
el merge en memoria requiere tener múltiples DataFrames grandes simultáneamente.

### Procedimiento por trimestres

Para cada trimestre:

1. Ajustar fechas en `samm_extract_data` y ejecutar los 3 extractores.
2. Copiar los Parquet al servidor: `app/data/`.
3. Ejecutar el DAG manualmente desde Airflow UI.
4. Esperar a que `pipeline_validation` quede en verde.
5. Continuar con el siguiente trimestre.

Los datos se acumulan correctamente — `ON CONFLICT DO NOTHING` garantiza que no haya duplicados entre runs.

| Trimestre      | fecha_inicio | fecha_fin        |
|----------------|--------------|------------------|
| Q1 2025        | 2025-01-01   | 2025-03-31       |
| Q2 2025        | 2025-04-01   | 2025-06-30       |
| Q3 2025        | 2025-07-01   | 2025-09-30       |
| Q4 2025        | 2025-10-01   | 2025-12-31       |
| 2026 hasta hoy | 2026-01-01   | *(fecha actual)* |

---

## Solución de Problemas

### SIGKILL -9 en `process_mobile_data` o `process_raw_mobile_data`

El OOM killer del sistema operativo terminó el proceso por falta de RAM. Usar la estrategia de carga por trimestres. No
hay límites de contenedor configurados — el límite es la RAM física del host.

### Error de conexión a PostgreSQL

```bash
# Verificar que el servicio esté corriendo
docker-compose ps postgres

# Ver logs
docker-compose logs postgres
```

### Archivos de datos no encontrados

```bash
# Verificar presencia y tamaño
ls -lah app/data/extract_*.parquet
ls -lah app/data/extract_datasource.parquet
```

### Timeout en tareas de Airflow

El timeout por tarea está configurado en 15 minutos (`AIRFLOW__CORE__TASK_TIMEOUT: 900`). Para volúmenes grandes,
aumentar este valor en `docker-compose.yaml`.

### Baja tasa de mapeo geográfico (<10%)

El `spatial_mapper.py` activa automáticamente un diagnóstico de rangos de coordenadas. Verificar que las coordenadas en
los datos de entrada estén dentro del bounding box de Ecuador (Lat: -6 a 3, Lon: -93 a -74).

### Vistas Grafana con NULL en Provincia/Cantón/Parroquia

Verificar que `update_raw_mappings` (paso 7) se haya ejecutado correctamente — este paso mapea las tablas `raw`, no las
`clean`. Si el porcentaje de mapeo es bajo, revisar la cobertura del shapefile y los rangos de coordenadas con el
diagnóstico automático.

### Catálogo de dispositivos sin Device/CZO

Verificar que `sense_nacional_v0.xlsx` esté presente en `app/data/` y que contenga las columnas `Device` y `CZO`.
Revisar el match por IMSI + IMEI entre `extract_datasource.parquet` y el Excel.

### Logs importantes

```bash
# DAG completo
docker-compose logs airflow-scheduler

# Tarea específica
docker-compose exec airflow-webserver airflow tasks logs \
  superset_etl_pipeline process_mobile_data <execution_date>

# Validación del pipeline
docker-compose exec airflow-webserver airflow tasks logs \
  superset_etl_pipeline pipeline_validation <execution_date>
```