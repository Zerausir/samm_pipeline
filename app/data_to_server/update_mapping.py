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


def update_mobile_mapping():
    """Update spatial mapping for mobile measurements only"""
    print("=== Actualizando mapeo espacial para datos móviles ===")

    spatial_mapper = get_spatial_mapper(
        host=get_env_var('POSTGRES_HOST'),
        port=get_env_var('POSTGRES_PORT'),
        database=get_env_var('POSTGRES_DB'),
        user=get_env_var('POSTGRES_USER'),
        password=get_env_var('POSTGRES_PASSWORD')
    )

    # Create mapping table if it doesn't exist
    spatial_mapper.create_mapping_table()

    # Process and populate mobile mappings
    count = spatial_mapper.process_and_map_locations()
    print(f"Created {count} mobile location mappings")
    return count


def update_voice_mapping():
    """Update spatial mapping for voice measurements only"""
    print("=== Actualizando mapeo espacial para datos de voz ===")

    spatial_mapper = get_spatial_mapper(
        host=get_env_var('POSTGRES_HOST'),
        port=get_env_var('POSTGRES_PORT'),
        database=get_env_var('POSTGRES_DB'),
        user=get_env_var('POSTGRES_USER'),
        password=get_env_var('POSTGRES_PASSWORD')
    )

    # Create mapping table if it doesn't exist
    spatial_mapper.create_mapping_table()

    # Process and populate voice mappings
    count = spatial_mapper.process_and_map_voice_locations()
    print(f"Created {count} voice location mappings")
    return count


def update_mapping():
    """Update spatial mapping for mobile measurements (legacy function)"""
    # Keep this function for backward compatibility
    return update_mobile_mapping()


def update_all_mappings():
    """Update spatial mapping for both mobile and voice measurements"""
    print("=== Actualizando mapeo espacial para todos los tipos de datos ===")

    spatial_mapper = get_spatial_mapper(
        host=get_env_var('POSTGRES_HOST'),
        port=get_env_var('POSTGRES_PORT'),
        database=get_env_var('POSTGRES_DB'),
        user=get_env_var('POSTGRES_USER'),
        password=get_env_var('POSTGRES_PASSWORD')
    )

    # Create mapping table if it doesn't exist
    spatial_mapper.create_mapping_table()

    # Process all location types
    total_count = spatial_mapper.process_all_locations()
    print(f"Total mappings created: {total_count}")
    return total_count


if __name__ == "__main__":
    # By default, update all mappings when run directly
    update_all_mappings()
