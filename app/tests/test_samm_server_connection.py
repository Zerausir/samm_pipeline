import os
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from dotenv import load_dotenv

load_dotenv()


def test_connection():
    try:
        # Build connection string
        connection_string = (
            f"DRIVER={os.getenv('DRIVER_NAME')};"
            f"SERVER={os.getenv('SERVER_NAME')};"
            f"DATABASE={os.getenv('DATABASE_NAME')};"
            "Trusted_Connection=yes;"
        )

        # Create connection URL
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})

        # Create engine
        engine = create_engine(connection_url)

        # Test connection with a simple query
        with engine.connect() as conn:
            result = conn.execute(sa.text("SELECT @@VERSION")).fetchone()
            print("Successfully connected to database!")
            print(f"SQL Server version: {result[0]}")

            # Test a simple table query
            tables = conn.execute(
                sa.text("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'"))
            print("\nAvailable tables:")
            for table in tables:
                print(f"- {table[0]}")

    except Exception as e:
        print(f"Connection failed with error:\n{str(e)}")


if __name__ == "__main__":
    test_connection()
