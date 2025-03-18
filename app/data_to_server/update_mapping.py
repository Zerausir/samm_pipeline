import os
from dotenv import load_dotenv
from app.utils.spatial_mapper import get_spatial_mapper


def update_mapping():
    load_dotenv()
    spatial_mapper = get_spatial_mapper(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )

    # Create mapping table
    spatial_mapper.create_mapping_table()

    # Process and populate mapping
    count = spatial_mapper.process_and_map_locations()
    print(f"Created {count} location mappings")


if __name__ == "__main__":
    update_mapping()
