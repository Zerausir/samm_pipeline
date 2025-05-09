import os
from dotenv import load_dotenv
from app.utils.spatial_mapper import get_spatial_mapper


def get_env_var(var_name, default=None):
    """Get environment variable, optionally loading from .env file"""
    value = os.environ.get(var_name)
    if value is None:
        # Only load from .env if the variable isn't already set
        load_dotenv()
        value = os.environ.get(var_name)
    return value if value is not None else default


def update_mapping():
    # Use get_env_var instead of calling load_dotenv()
    spatial_mapper = get_spatial_mapper(
        host=get_env_var('POSTGRES_HOST'),
        port=get_env_var('POSTGRES_PORT'),
        database=get_env_var('POSTGRES_DB'),
        user=get_env_var('POSTGRES_USER'),
        password=get_env_var('POSTGRES_PASSWORD')
    )

    # Create mapping table
    spatial_mapper.create_mapping_table()

    # Process and populate mapping
    count = spatial_mapper.process_and_map_locations()
    print(f"Created {count} location mappings")


if __name__ == "__main__":
    update_mapping()
