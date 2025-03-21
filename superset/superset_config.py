# superset/superset_config.py
from environs import Env
from sqlalchemy.engine.url import URL

env = Env()
env.read_env()

# Crear URL de conexión
url = URL.create(
    drivername="postgresql+psycopg2",
    username=env("POSTGRES_USER"),
    password=env("POSTGRES_PASSWORD"),
    host=env("POSTGRES_HOST"),
    port=env("POSTGRES_PORT"),
    database=env("POSTGRES_DB")
)

SECRET_KEY = env("SUPERSET_SECRET_KEY")
SQLALCHEMY_DATABASE_URI = str(url)
SQLALCHEMY_TRACK_MODIFICATIONS = False
APP_NAME = "Superset"
WTF_CSRF_ENABLED = True
WTF_CSRF_SSL_STRICT = True

# Configuración HTTPS
ENABLE_PROXY_FIX = True
WEBSERVER_PROTOCOL = env("SUPERSET_WEBSERVER_PROTOCOL", default="https")
WEBSERVER_ADDRESS = env("SUPERSET_WEBSERVER_ADDRESS", default="0.0.0.0")
PREFERRED_URL_SCHEME = "https"

# Habilitar JavaScript y controles avanzados
ENABLE_JAVASCRIPT_CONTROLS = True

# Habilitar todas las features disponibles
FEATURE_FLAGS = {
    # Características de exploración y visualización
    'ENABLE_JAVASCRIPT_CONTROLS': True,
    'ENABLE_TEMPLATE_PROCESSING': True,
    'ENABLE_EXPLORE_JSON_ENDPOINTS': True,
    'ENABLE_EXPLORE_DRAG_AND_DROP': True,
    'ENABLE_ADVANCED_DATA_CONTROLS': True,
    'DASHBOARD_FILTERS_EXPERIMENTAL': True,
    'DASHBOARD_CROSS_FILTERS': True,
    'DASHBOARD_NATIVE_FILTERS': True,
    'DASHBOARD_NATIVE_FILTERS_SET': True,
    'DASHBOARD_VIRTUALIZATION': True,
    'VERSIONED_EXPORT': True,
    'ENABLE_TEMPLATE_REMOVE_FILTERS': True,
    'MAP_VIEW_SYNC_ENABLED': True,

    # Características de mapas
    'SCOPED_FILTER': True,
    'ALLOW_FULL_CSV_EXPORT': True,
    'UX_BETA': True,

    # Funcionalidades avanzadas
    'ALERT_REPORTS': True,
    'EMBEDDED_SUPERSET': True,
    'EMBEDDABLE_CHARTS': True,
    'THUMBNAILS': True,
    'LISTVIEWS_DEFAULT_CARD_VIEW': True,
    'DASHBOARD_RBAC': True,
    'SSH_TUNNELING': True,
}

# Cache config
CACHE_CONFIG = {
    'CACHE_TYPE': 'SimpleCache',
}

# Additional settings
SUPERSET_WEBSERVER_PORT = 8088
SUPERSET_WEBSERVER_TIMEOUT = 60
SUPERSET_LOAD_EXAMPLES = False
SQLLAB_TIMEOUT = 30
MAPBOX_API_KEY = env("MAPBOX_API_KEY")

# Configuraciones de visualización avanzadas
SUPERSET_VIZ_TYPE_BLACKLIST = []  # No bloquear ningún tipo de visualización
ENABLE_SCHEDULED_EMAIL_REPORTS = True
