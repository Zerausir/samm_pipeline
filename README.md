# sma_superset

Pipeline ETL completo para procesamiento de datos de telecomunicaciones móviles y de voz, orquestado con Apache Airflow
y almacenado en PostgreSQL para análisis en PowerBI.

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
- [Administración](#administración)
- [Carga Histórica Inicial](#carga-histórica-inicial)
- [Solución de Problemas](#solución-de-problemas)

---

## Descripción

Este pipeline recibe archivos Parquet generados por [
`samm_extract_data`](https://github.com/Zerausir/samm_extract_data), los procesa, los almacena en PostgreSQL y crea
vistas optimizadas para su consumo en PowerBI. El sistema corre completamente dockerizado con Apache Airflow como
orquestador.

### Flujo completo de datos

```
[SQL Server] ──► [samm_extract_data] ──► [Archivos Parquet]
                    (máquina AD)
                                               │
                                               ▼
                                        [sma_superset]
                                               │
                              ┌────────────────┼────────────────┐
                              ▼                ▼                ▼
                         Airflow DAG     PostgreSQL        PowerBI
                         (orquesta)     (almacena)       (visualiza)
```

---

## Arquitectura

### Servicios Docker

| Servicio            | Imagen                 | Función                                |
|---------------------|------------------------|----------------------------------------|
| `postgres`          | `postgres:17-alpine`   | Base de datos principal                |
| `airflow-webserver` | `custom-airflow:2.7.3` | UI de monitoreo (puerto 8080)          |
| `airflow-scheduler` | `custom-airflow:2.7.3` | Orquestador del DAG                    |
| `airflow-init`      | `custom-airflow:2.7.3` | Inicialización única del ambiente      |
| `data-processor`    | `python:3.11`          | Contenedor auxiliar para procesamiento |

### DAG: `superset_etl_pipeline`

El pipeline es **secuencial** — cada paso depende del anterior:

```
process_mobile_data
        │
        ▼
process_voice_data
        │
        ▼
update_all_mappings
        │
        ▼
create_mobile_views
        │
        ▼
create_voice_views
        │
        ▼
pipeline_validation
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

> ⚠️ **Sin límites de memoria en contenedores**: los contenedores de Airflow usan toda la RAM disponible del host. Para
> volúmenes de datos grandes (>6 meses de historia), ver [Carga Histórica Inicial](#carga-histórica-inicial).

---

## Estructura del Proyecto

```
sma_superset/
├── airflow/
│   ├── dags/
│   │   └── superset_etl_dag.py          # DAG principal con 6 pasos
│   ├── logs/                            # Logs de ejecución (auto-generado)
│   └── plugins/
├── app/
│   ├── data/                            # Archivos Parquet y estáticos (NO en git)
│   │   ├── extract_datos_table1.parquet
│   │   ├── extract_datos_table2.parquet
│   │   ├── extract_voz_table1.parquet
│   │   ├── extract_voz_table3.parquet
│   │   ├── extract_voz_table4.parquet
│   │   ├── sense_nacional_v0.xlsx
│   │   └── states/                      # Shapefile de Ecuador
│   │       ├── shapefile.shp
│   │       ├── shapefile.shx
│   │       ├── shapefile.dbf
│   │       └── shapefile.prj
│   ├── data_to_server/
│   │   ├── main.py                      # ETL datos móviles
│   │   ├── main_voice.py                # ETL datos de voz
│   │   ├── update_mapping.py            # Mapeo espacial
│   │   ├── create_dashboard_view.py     # Vista móvil para PowerBI
│   │   └── create_dashboard_view_voice.py # Vista voz para PowerBI
│   └── utils/
│       ├── postgres_handler.py          # Handler PostgreSQL optimizado
│       └── spatial_mapper.py           # Mapeador espacial con STRtree
├── postgres-conf/
│   └── postgresql.conf                  # Configuración PostgreSQL
├── excel_para_fabric/
│   └── transfer.py                      # Exportación a Excel (auxiliar)
├── Dockerfile.airflow                   # Imagen personalizada de Airflow
├── docker-compose.yaml
├── .env                                 # Variables de entorno (NO en git)
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
```

> 🔒 El archivo `.env` está en `.gitignore`. Nunca lo incluyas en un commit.

### Archivos de datos requeridos

Antes de ejecutar el pipeline, los siguientes archivos deben estar presentes en `app/data/`:

| Archivo                                    | Origen                           |
|--------------------------------------------|----------------------------------|
| `extract_datos_table1.parquet`             | Generado por `samm_extract_data` |
| `extract_datos_table2.parquet`             | Generado por `samm_extract_data` |
| `extract_voz_table1.parquet`               | Generado por `samm_extract_data` |
| `extract_voz_table3.parquet`               | Generado por `samm_extract_data` |
| `extract_voz_table4.parquet`               | Generado por `samm_extract_data` |
| `sense_nacional_v0.xlsx`                   | Provisto manualmente             |
| `states/shapefile.shp` (+.shx, .dbf, .prj) | Provisto manualmente             |

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

La imagen incluye dependencias adicionales (geopandas, pyarrow, psycopg2, shapely):

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

Abrir http://localhost:8080 con las credenciales configuradas en `.env`.

---

## Pipeline ETL

### Paso 1 — `process_mobile_data`

**Script**: `app/data_to_server/main.py`

Carga `extract_datos_table1.parquet` y `extract_datos_table2.parquet`, aplica filtros (solo sesiones HTTP
Download/Post), elimina duplicados, realiza merge entre tablas, calcula throughput en Mbps, filtra registros sin
coordenadas válidas y almacena en `mobile_measurements`.

### Paso 2 — `process_voice_data`

**Script**: `app/data_to_server/main_voice.py`

Carga los 3 archivos Parquet de voz, filtra sesiones Voice MO, realiza merge entre tablas, filtra registros sin
coordenadas válidas y almacena en `voice_measurements`.

### Paso 3 — `update_all_mappings`

**Script**: `app/data_to_server/update_mapping.py`

Para cada medición sin mapeo geográfico, determina a qué parroquia/cantón/provincia pertenece usando un índice espacial
R-tree sobre el shapefile de Ecuador, e inserta en `location_mapping`.

### Paso 4 — `create_mobile_views`

**Script**: `app/data_to_server/create_dashboard_view.py`

Recrea la vista `data_dashboard_view_visualization` con todas las columnas de `mobile_measurements` más la información
geográfica (provincia, cantón, parroquia). La vista se elimina y recrea en cada ejecución para reflejar cambios de
schema.

### Paso 5 — `create_voice_views`

**Script**: `app/data_to_server/create_dashboard_view_voice.py`

Ídem para datos de voz: vista `voice_dashboard_view_visualization`.

### Paso 6 — `pipeline_validation`

Valida integridad completa: conteo de registros en tablas y vistas, porcentaje de mapeo geográfico (umbral mínimo 50%),
existencia de vistas. Genera reporte de estado.

---

## Base de Datos

### Tablas principales

#### `mobile_measurements`

Almacena mediciones móviles procesadas. Columnas base fijas + columnas dinámicas agregadas automáticamente según el
schema del Parquet de entrada.

| Columna                | Tipo                               | Descripción                        |
|------------------------|------------------------------------|------------------------------------|
| `measurement_id`       | `VARCHAR` PK                       | MD5 determinístico de campos clave |
| `valid_from`           | `TIMESTAMP`                        | Timestamp de inserción             |
| `is_current`           | `INTEGER`                          | Flag de registro activo (1)        |
| `batch_id`             | `VARCHAR`                          | ID del batch de inserción          |
| *(columnas dinámicas)* | `TEXT` / `DOUBLE PRECISION` / etc. | Todas las columnas del Parquet     |

#### `voice_measurements`

Misma estructura que `mobile_measurements` para datos de voz.

#### `geographic_regions`

Regiones geográficas del shapefile de Ecuador (1 081 parroquias). Se inserta una sola vez al inicio y no se modifica.

#### `location_mapping`

Mapa entre `measurement_id` y `region_id`. Clave compuesta `(measurement_id, region_id, location_type)`.

### Vistas para PowerBI

```sql
-- Datos móviles con información geográfica
SELECT * FROM data_dashboard_view_visualization;

-- Datos de voz con información geográfica
SELECT * FROM voice_dashboard_view_visualization;
```

### Índices

```sql
-- mobile_measurements
idx_mobile_batch_id, idx_mobile_valid_from, idx_mobile_is_current, idx_mobile_ingestion

-- voice_measurements
idx_voice_batch_id, idx_voice_valid_from, idx_voice_is_current, idx_voice_ingestion

-- geographic_regions
idx_geo_batch_id, idx_geo_valid_from, idx_geo_is_current, idx_geo_despro, idx_geo_descan

-- location_mapping
idx_location_mapping_measurement, idx_location_mapping_region, idx_location_mapping_type
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

# Verificar uso de recursos
docker stats
```

## Carga Histórica Inicial

Procesar más de ~6 meses de datos en una sola ejecución puede causar `SIGKILL -9` (OOM del sistema operativo) dado que
el merge en memoria requiere tener múltiples DataFrames grandes simultáneamente. La estrategia recomendada es cargar por
trimestres:

### Procedimiento

Para cada trimestre:

1. Ajustar fechas en `samm_extract_data` y ejecutar los extractores
2. Copiar los Parquet al servidor: `app/data/`
3. Ejecutar el DAG manualmente desde Airflow
4. Esperar a que el pipeline complete (`pipeline_validation` en verde)
5. Continuar con el siguiente trimestre

Los datos se acumulan correctamente — el mecanismo `ON CONFLICT DO NOTHING` garantiza que no haya duplicados entre runs.

| Trimestre      | fecha_inicio | fecha_fin        |
|----------------|--------------|------------------|
| Q1 2025        | 2025-01-01   | 2025-03-31       |
| Q2 2025        | 2025-04-01   | 2025-06-30       |
| Q3 2025        | 2025-07-01   | 2025-09-30       |
| Q4 2025        | 2025-10-01   | 2025-12-31       |
| 2026 hasta hoy | 2026-01-01   | *(fecha actual)* |

Una vez completada la carga inicial, el pipeline vuelve a su operación normal con el rango de 166 días configurado en
`samm_extract_data`.

---

## Solución de Problemas

### SIGKILL -9 en `process_mobile_data`

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
```

### Timeout en tareas de Airflow

El timeout por tarea está configurado en 15 minutos (`AIRFLOW__CORE__TASK_TIMEOUT: 900`). Para volúmenes grandes,
aumentar este valor en `docker-compose.yaml`.

### Baja tasa de mapeo geográfico (<10%)

El `spatial_mapper.py` activa automáticamente un diagnóstico de rangos de coordenadas. Verificar que las coordenadas en
los datos de entrada estén dentro del bounding box de Ecuador (Lat: -6 a 3, Lon: -93 a -74).

### Logs importantes

```bash
# DAG completo
docker-compose logs airflow-scheduler

# Tarea específica
docker-compose exec airflow-webserver airflow tasks logs \
  superset_etl_pipeline process_mobile_data <execution_date>
```