# Superset ETL Pipeline

Un pipeline completo de ETL desarrollado con Apache Airflow para procesar datos móviles y de voz, con visualización en
PowerBI.

## 🏗️ Arquitectura del Sistema

Este proyecto implementa un sistema completo de procesamiento de datos de telecomunicaciones que consta de dos etapas
principales:

### Flujo Completo de Datos

```
[SQL Server] → [Extractor AD] → [Archivos Parquet] → [ETL Pipeline] → [PostgreSQL] → [PowerBI]
     ↓              ↓               ↓                    ↓              ↓            ↓
  Datos Raw    Usuario AD     Datos Extraídos      Procesamiento    Vistas      Análisis
                Requerido     (.parquet)           Dockerizado    Optimizadas   Final
```

### Componentes del Sistema

#### 1. **Extractor de Datos** (Prerequisito)

- **Ubicación**: Proceso separado ejecutado en usuario con Active Directory
- **Función**: Extrae datos desde SQL Server usando autenticación Windows
- **Salida**: Archivos Parquet listos para procesamiento
- **Repositorio**: `samm_extract_data/` (proceso independiente)

#### 2. **Pipeline ETL** (Este proyecto)

- **Apache Airflow**: Orquestación y scheduling del pipeline
- **PostgreSQL**: Base de datos principal para almacenamiento
- **Python ETL**: Procesamiento de datos con pandas y geopandas
- **PowerBI**: Visualización final de datos

### Dependencias entre Procesos

⚠️ **IMPORTANTE**: El ETL Pipeline requiere que el proceso de extracción se haya ejecutado previamente y los archivos
Parquet estén disponibles en `/app/data/`.

## 📋 Requisitos del Sistema

### Software Requerido

- Docker Desktop
- Docker Compose
- Python 3.11+
- Al menos 8GB de RAM
- 20GB de espacio en disco

### ⚠️ Prerequisitos Críticos

#### 1. Proceso de Extracción Completado

Antes de ejecutar este pipeline ETL, debe haberse ejecutado el **proceso de extracción de datos** desde un usuario con
permisos de Active Directory. Este proceso genera los archivos Parquet necesarios.

**Repositorio de Extracción**: `samm_extract_data/`

**Archivos Requeridos** (generados por el extractor):

- `extract_datos_table1.parquet` - Datos móviles tabla 1
- `extract_datos_table2.parquet` - Datos móviles tabla 2
- `extract_voz_table1.parquet` - Datos de voz tabla 1
- `extract_voz_table3.parquet` - Datos de voz tabla 3
- `extract_voz_table4.parquet` - Datos de voz tabla 4

#### 2. Archivos Adicionales Requeridos

El sistema también espera los siguientes archivos en `/app/data/`:

- `sense_nacional_v0.xlsx` - Datos de dispositivos
- `states/shapefile.shp` - Archivo geográfico (con archivos asociados .shx, .dbf, .prj)

### Verificación de Archivos

Antes de iniciar el pipeline, verificar que todos los archivos estén presentes:

```bash
ls -la app/data/
# Debe mostrar:
# extract_datos_table1.parquet
# extract_datos_table2.parquet
# extract_voz_table1.parquet
# extract_voz_table3.parquet
# extract_voz_table4.parquet
# sense_nacional_v0.xlsx
# states/shapefile.shp (y archivos asociados)
```

## 🚀 Instalación y Configuración

### Paso 0: Ejecutar Proceso de Extracción

⚠️ **PREREQUISITO OBLIGATORIO**: Antes de continuar, debe ejecutar el proceso de extracción de datos desde un usuario
con acceso al Active Directory.

1. **Cambiar al repositorio de extracción**:
   ```bash
   cd samm_extract_data/
   ```

2. **Configurar variables de entorno del extractor** (crear `.env` en `samm_extract_data/`):
   ```env
   SERVER_NAME=tu_servidor_sql
   DATABASE_NAME=tu_base_de_datos
   DRIVER_NAME={ODBC Driver 17 for SQL Server}
   TABLE1=esquema.tabla_sesiones
   TABLE2=esquema.tabla_datos_sesion
   TABLE3=esquema.tabla_voz_sesion
   TABLE4=esquema.tabla_calidad_voz
   EXTRACT_DATA_OUTPUT_DIR=C:/ruta/completa/al/directorio/de/salida
   ```

3. **Ejecutar extracciones**:
   ```bash
   # Extraer datos móviles
   python extract_data_datos.py
   
   # Extraer datos de voz
   python extract_data_voz.py
   ```

4. **Copiar archivos extraídos al pipeline ETL**:
   ```bash
   # Copiar archivos parquet al directorio del pipeline ETL
   cp [EXTRACT_DATA_OUTPUT_DIR]/*.parquet /ruta/al/pipeline/app/data/
   ```

### 1. Clonar el Repositorio del Pipeline ETL

```bash
git clone <repository-url>
cd sma_superset
```

### 2. Configurar Variables de Entorno

Crear archivo `.env` basado en `.env.example`:

```bash
cp .env.example .env
```

Configurar las siguientes secciones en `.env`:

#### Variables SQL Server

```env
DRIVER_NAME='ODBC Driver 17 for SQL Server'
SERVER_NAME='your_server'
DATABASE_NAME='your_database'
TABLE1='table1_name'
TABLE2='table2_name'
```

#### Variables PostgreSQL

```env
POSTGRES_HOST='your_postgres_host'
POSTGRES_PORT='5432'
POSTGRES_DB='your_database'
POSTGRES_USER='your_user'
POSTGRES_PASSWORD='your_password'
```

#### Variables Airflow

```env
AIRFLOW_UID='50000'
_AIRFLOW_WWW_USER_USERNAME='admin'
_AIRFLOW_WWW_USER_PASSWORD='your_password'
```

#### Rutas de Archivos

```env
sense_file='/app/data/sense_nacional_v0.xlsx'
geojson_route='/app/data/states'
```

### 3. Construir Imagen Personalizada de Airflow

```bash
docker build -f Dockerfile.airflow -t custom-airflow:2.7.3 .
```

### 4. Inicializar Servicios

```bash
# Crear directorios necesarios
mkdir -p airflow/dags airflow/logs airflow/plugins app/data

# Configurar permisos
echo -e "AIRFLOW_UID=$(id -u)" > .env.local

# Inicializar Airflow
docker-compose up airflow-init

# Iniciar servicios
docker-compose up -d
```

## 📊 Pipeline ETL

### Flujo de Datos

El pipeline ejecuta las siguientes etapas secuencialmente:

1. **Procesamiento Móvil** (`process_mobile_data`)
    - Carga archivos parquet móviles
    - Procesa y limpia datos
    - Calcula throughput
    - Almacena en `mobile_measurements`

2. **Procesamiento Voz** (`process_voice_data`)
    - Carga archivos parquet de voz
    - Merge de múltiples tablas
    - Limpia y valida datos de llamadas
    - Almacena en `voice_measurements`

3. **Mapeo Espacial** (`update_all_mappings`)
    - Procesa coordenadas de ambos tipos de datos
    - Mapea puntos a regiones geográficas
    - Actualiza tabla `location_mapping`

4. **Vista Móvil** (`create_mobile_views`)
    - Crea `data_dashboard_view_visualization`
    - Incluye datos geográficos
    - Optimizada para PowerBI

5. **Vista Voz** (`create_voice_views`)
    - Crea `voice_dashboard_view_visualization`
    - Incluye datos geográficos
    - Optimizada para PowerBI

6. **Validación Final** (`pipeline_validation`)
    - Valida integridad de datos
    - Verifica mapeos espaciales
    - Genera reporte completo

### Programación

- **Horario**: Diariamente a las 9:00 y 14:00
- **Modo**: Secuencial (sin paralelización)
- **Timeout**: 15 minutos por tarea

## 🗄️ Estructura de Base de Datos

### Tablas Principales

#### `mobile_measurements`

Almacena datos móviles procesados con todas las columnas originales más metadatos de versionado.

#### `voice_measurements`

Almacena datos de voz procesados con todas las columnas originales más metadatos de versionado.

#### `geographic_regions`

Contiene información geográfica con geometrías en formato JSONB.

#### `location_mapping`

Mapea mediciones a regiones geográficas.

### Vistas para PowerBI

#### `data_dashboard_view_visualization`

Vista optimizada para análisis móvil que incluye:

- Todas las columnas de datos móviles
- Información geográfica (provincia, cantón, parroquia)
- Datos de throughput calculados

#### `voice_dashboard_view_visualization`

Vista optimizada para análisis de voz que incluye:

- Todas las columnas de datos de voz
- Información geográfica
- Métricas de calidad de llamadas

## 🔧 Administración

### Acceso a Servicios

#### Airflow Web UI

- **URL**: http://localhost:8080
- **Usuario**: Configurado en `.env`
- **Password**: Configurado en `.env`

#### PostgreSQL

- **Host**: localhost
- **Puerto**: 5432
- **Base de datos**: airflow (para Airflow), configurar según `.env` para datos

### Comandos Útiles

#### Ver logs del pipeline

```bash
docker-compose logs -f airflow-scheduler
```

#### Ejecutar DAG manualmente

```bash
docker-compose exec airflow-webserver airflow dags trigger superset_etl_pipeline
```

#### Acceder a contenedor de Airflow

```bash
docker-compose exec airflow-webserver bash
```

#### Reiniciar servicios

```bash
docker-compose restart
```

### Monitoreo

#### Verificar estado de servicios

```bash
docker-compose ps
```

#### Revisar uso de recursos

```bash
docker stats
```

#### Verificar datos en PostgreSQL

```bash
docker-compose exec postgres psql -U airflow -d airflow -c "SELECT COUNT(*) FROM mobile_measurements WHERE is_current = 1;"
```

## 📈 Conexión a PowerBI

### Configuración de Conexión

1. **Tipo de Conexión**: PostgreSQL
2. **Servidor**: [Tu host PostgreSQL]
3. **Puerto**: 5432
4. **Base de datos**: [Configurada en POSTGRES_DB]
5. **Autenticación**: Base de datos
6. **Usuario/Contraseña**: [Configurados en .env]

### Tablas Recomendadas

Para análisis móvil:

```sql
SELECT * FROM data_dashboard_view_visualization
```

Para análisis de voz:

```sql
SELECT * FROM voice_dashboard_view_visualization
```

### Campos Clave

#### Datos Móviles

- `ThroughputMbps`: Velocidad calculada
- `StartLatitude`, `StartLongitude`: Coordenadas de inicio
- `dpa_despro`, `dpa_descan`: Información geográfica

#### Datos de Voz

- `CallDirection`: Dirección de llamada
- `AqmSessionEndAqmCallQuality`: Calidad de llamada
- Coordenadas geográficas y mapeo regional

## 🛠️ Desarrollo

### Estructura de Código

```
sma_superset/
├── airflow/
│   ├── dags/           # DAGs de Airflow
│   ├── logs/           # Logs de ejecución
│   └── plugins/        # Plugins personalizados
├── app/
│   ├── data_to_server/ # Scripts ETL principales
│   └── utils/          # Utilidades y handlers
├── postgres-conf/      # Configuración PostgreSQL
└── docker-compose.yaml # Configuración de servicios
```

### Agregar Nuevos Pasos al Pipeline

1. Crear función en `app/data_to_server/`
2. Importar en el DAG
3. Crear PythonOperator
4. Agregar a la cadena de dependencias

### Personalizar Procesamiento

Modificar las funciones en:

- `main.py`: Procesamiento móvil
- `main_voice.py`: Procesamiento de voz
- `update_mapping.py`: Mapeo geográfico

## 🐛 Troubleshooting

### Problemas Comunes

#### Error de conexión a PostgreSQL

```bash
# Verificar que PostgreSQL esté corriendo
docker-compose ps postgres

# Revisar logs
docker-compose logs postgres
```

#### Timeouts en tareas

- Aumentar `AIRFLOW__CORE__TASK_TIMEOUT` en docker-compose.yaml
- Verificar recursos del sistema

#### Archivos de datos no encontrados

- Verificar que los archivos estén en `/app/data/`
- Revisar permisos de archivos

#### Memoria insuficiente

- Aumentar memoria asignada a Docker
- Reducir `BATCH_SIZE` en scripts ETL

### Logs Importantes

```bash
# Logs del DAG
docker-compose logs airflow-scheduler

# Logs de tareas específicas
docker-compose exec airflow-webserver airflow tasks logs superset_etl_pipeline [task_id] [execution_date]

# Logs de base de datos
docker-compose logs postgres
```

### Problemas Específicos del Flujo de Datos

#### Archivos de extracción desactualizados

```bash
# Verificar fechas de los archivos parquet
ls -la app/data/*.parquet

# Si están desactualizados, ejecutar nuevamente el proceso de extracción
cd samm_extract_data/
python extract_data_datos.py
python extract_data_voz.py
```

#### Inconsistencias entre extracción y ETL

- Verificar que las fechas de extracción coincidan con el período esperado
- Revisar los archivos de metadatos generados por el extractor:
    - `extraction_datos_metadata.json`
    - `extraction_voz_metadata.json`

#### Coordinación de procesos

- El proceso de extracción debe ejecutarse antes que el ETL
- Verificar que los archivos estén completamente escritos antes de iniciar ETL
- Considerar implementar verificaciones de integridad de archivos

## 📝 Configuración Adicional

### Flujo Completo de Trabajo

Para ejecutar el proceso completo de datos, seguir este orden:

#### 1. Fase de Extracción (Usuario AD)

```bash
# En máquina con acceso AD
cd samm_extract_data/
python extract_data_datos.py    # Extrae datos móviles
python extract_data_voz.py      # Extrae datos de voz
```

#### 2. Transferencia de Datos

```bash
# Copiar archivos al servidor del pipeline ETL
scp extract_*.parquet usuario@servidor:/ruta/pipeline/app/data/
# O usar el método de transferencia apropiado para tu entorno
```

#### 3. Fase de Procesamiento (Pipeline ETL)

```bash
# En servidor del pipeline
cd sma_superset/
docker-compose up -d
# El DAG se ejecutará automáticamente según programación
```

### Automatización Recomendada

Para automatizar el proceso completo, considerar:

1. **Script de coordinación** que ejecute extracción → transferencia → ETL
2. **Validaciones de integridad** entre etapas
3. **Notificaciones** de estado de cada fase
4. **Rollback automático** en caso de errores

### Variables de Entorno Requeridas

Crear un archivo `.env` en la raíz del proyecto con las siguientes variables:

```env
# Variables para conexión SQL Server
DRIVER_NAME='ODBC Driver 17 for SQL Server'
SERVER_NAME='tu_servidor'
DATABASE_NAME='tu_base_datos'
TABLE1='tabla1'
TABLE2='tabla2'

# Rutas para el contenedor
sense_file='/app/data/sense_nacional_v0.xlsx'
geojson_route='/app/data/states'

# Configuración de PostgreSQL
POSTGRES_HOST='tu_host_postgres'
POSTGRES_PORT='5432'
POSTGRES_DB='tu_base_datos'
POSTGRES_USER='tu_usuario'
POSTGRES_PASSWORD='tu_contraseña'

# Configuración de Superset
SECRET_KEY='tu_clave_secreta'
FLASK_APP='superset.app:create_app()'
SUPERSET_CONFIG_PATH='/app/pythonpath/superset_config.py'
SUPERSET_SECRET_KEY='tu_clave_superset'
MAPBOX_API_KEY='tu_api_key_mapbox'

# Configuración de Airflow
AIRFLOW_UID='50000'
_AIRFLOW_WWW_USER_USERNAME='admin'
_AIRFLOW_WWW_USER_PASSWORD='tu_contraseña_admin'
```

### Preparación de Datos

#### Archivos Generados por el Extractor

Los siguientes archivos deben estar presentes en `app/data/` **antes** de ejecutar el pipeline ETL:

1. **Archivos Parquet de Datos Móviles** (generados por `extract_data_datos.py`):
    - `extract_datos_table1.parquet`
    - `extract_datos_table2.parquet`

2. **Archivos Parquet de Datos de Voz** (generados por `extract_data_voz.py`):
    - `extract_voz_table1.parquet`
    - `extract_voz_table3.parquet`
    - `extract_voz_table4.parquet`

3. **Archivos de Metadatos** (generados automáticamente):
    - `extraction_datos_metadata.json`
    - `extraction_voz_metadata.json`

#### Archivos Adicionales Requeridos

4. **Archivo Excel de Dispositivos** (provisto manualmente):
    - `sense_nacional_v0.xlsx`

5. **Archivos Geográficos** (provistos manualmente):
    - `states/shapefile.shp` (y archivos asociados .shx, .dbf, .prj)

#### Validación de Archivos

Antes de iniciar el ETL, ejecutar:

```bash
# Verificar presencia de archivos
ls -la app/data/extract_*.parquet
ls -la app/data/sense_nacional_v0.xlsx
ls -la app/data/states/shapefile.*

# Verificar tamaños (los archivos no deben estar vacíos)
du -h app/data/extract_*.parquet
```

## 🔄 Flujo de Ejecución

### Arquitectura de Dos Fases

#### Fase 1: Extracción (Usuario AD Requerido)

1. **Prerequisitos**:
    - Usuario con permisos de Active Directory
    - Acceso a SQL Server con autenticación Windows
    - Repositorio `samm_extract_data/` configurado

2. **Proceso**:
    - Extrae datos desde SQL Server (2025-01-01 hasta presente)
    - Genera archivos Parquet optimizados
    - Crea metadatos de extracción

3. **Salida**:
    - 5 archivos `.parquet` con datos limpios
    - 2 archivos `.json` con metadatos

#### Fase 2: Pipeline ETL (Dockerizado)

1. **Entrada**:
    - Archivos Parquet de la Fase 1
    - Archivos geográficos y de dispositivos

2. **Procesamiento Secuencial**:
    - **Extracción y Procesamiento** → Carga y limpia datos fuente
    - **Transformación** → Aplica reglas de negocio y cálculos
    - **Mapeo Geográfico** → Asocia coordenadas con regiones
    - **Creación de Vistas** → Genera vistas optimizadas para análisis
    - **Validación** → Verifica integridad y calidad de datos

3. **Salida**:
    - Datos estructurados en PostgreSQL
    - Vistas optimizadas para PowerBI

### Programación y Scheduling

- **Extracción**: Manual o programada vía cron/Task Scheduler
- **ETL Pipeline**: Automático via Airflow (9:00 y 14:00 diariamente)
- **Coordinación**: La extracción debe completarse antes del ETL

## 📊 Métricas y Monitoring

El sistema incluye validación automática que reporta:

- Número de registros procesados por tipo de datos
- Porcentaje de éxito en mapeo geográfico
- Calidad de datos (registros con coordenadas válidas)
- Estado de las vistas generadas
- Tiempo de ejecución por etapa

## 🔒 Seguridad

- Las credenciales se configuran a través de variables de entorno
- Conexiones de base de datos utilizan autenticación por usuario/contraseña
- Los logs no exponen información sensible
- El acceso a Airflow requiere autenticación

## 📄 Licencia

Este proyecto está bajo la Licencia MIT. Ver archivo `LICENSE` para más detalles.

---

Para soporte técnico o preguntas específicas, consultar la documentación de cada componente o crear un issue en el
repositorio.