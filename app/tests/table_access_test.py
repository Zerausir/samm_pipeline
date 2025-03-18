import os
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from dotenv import load_dotenv

load_dotenv()

def test_table_access():
    try:
        connection_string = (
            f"DRIVER={os.getenv('DRIVER_NAME')};"
            f"SERVER={os.getenv('SERVER_NAME')};"
            f"DATABASE={os.getenv('DATABASE_NAME')};"
            "Trusted_Connection=yes;"
        )

        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        engine = create_engine(connection_url)

        fecha_inicio = "2024-11-12 00:01:00"
        fecha_fin = "2024-11-12 23:59:00"

        query = f"""
            SELECT * 
            FROM {os.getenv('TABLE1')}
            WHERE StartTime >= '{fecha_inicio}' 
            AND EndTime <= '{fecha_fin}'
        """

        with engine.connect() as conn:
            df = pd.read_sql_query(sa.text(query), conn)
            df.to_csv("datos_exportados.csv", index=False, sep=';', decimal='.')
            print(f"\nNúmero de filas exportadas: {len(df)}")

    except Exception as e:
        print(f"Error en la exportación:\n{str(e)}")

if __name__ == "__main__":
    test_table_access()