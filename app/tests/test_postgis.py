import unittest
from dotenv import load_dotenv
import os
from app.utils.postgres_handler import PostgresDataHandler

class TestPostGIS(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        load_dotenv()
        cls.handler = PostgresDataHandler({
            'host': os.getenv('POSTGRES_HOST'),
            'port': os.getenv('POSTGRES_PORT'),
            'database': os.getenv('POSTGRES_DB'),
            'user': os.getenv('POSTGRES_USER'),
            'password': os.getenv('POSTGRES_PASSWORD')
        })

    def test_postgis_installed(self):
        """Test if PostGIS extension is installed and working"""
        is_installed = self.handler.test_postgis()
        self.assertTrue(is_installed, "PostGIS is not installed")

    def test_spatial_functions(self):
        """Test basic PostGIS spatial functions"""
        with self.handler._get_connection() as conn:
            with conn.cursor() as cur:
                try:
                    # Test point creation
                    cur.execute("SELECT ST_AsText(ST_Point(-78.4, -0.2));")
                    point = cur.fetchone()[0]
                    self.assertTrue(point.startswith("POINT"), "Failed to create point")

                    # Test basic spatial operation
                    cur.execute("""
                        SELECT ST_Contains(
                            ST_GeomFromText('POLYGON((-79 1, -79 -1, -77 -1, -77 1, -79 1))'),
                            ST_Point(-78.4, -0.2)
                        );
                    """)
                    contains = cur.fetchone()[0]
                    self.assertTrue(contains, "Spatial containment test failed")

                except Exception as e:
                    self.fail(f"PostGIS spatial functions test failed: {str(e)}")

if __name__ == '__main__':
    unittest.main()