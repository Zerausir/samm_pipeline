from airflow.hooks.base import BaseHook
import psycopg2
from psycopg2.extras import RealDictCursor


class PostgreSQLCustomHook(BaseHook):
    """
    Hook personalizado para interactuar con PostgreSQL.
    Extiende la funcionalidad básica del hook PostgresHook de Airflow.

    :param postgres_conn_id: ID de la conexión de Airflow a PostgreSQL
    """

    def __init__(self, postgres_conn_id='postgres_default'):
        self.postgres_conn_id = postgres_conn_id
        self.connection = None

    def get_conn(self):
        """Obtiene la conexión a PostgreSQL"""
        if self.connection is not None:
            return self.connection

        conn_info = self.get_connection(self.postgres_conn_id)
        conn_args = {
            'host': conn_info.host,
            'user': conn_info.login,
            'password': conn_info.password,
            'dbname': conn_info.schema,
            'port': conn_info.port
        }

        self.connection = psycopg2.connect(**conn_args)
        return self.connection

    def run_query(self, sql, parameters=None):
        """
        Ejecuta una consulta SQL y devuelve los resultados como diccionarios.

        :param sql: Consulta SQL a ejecutar
        :param parameters: Parámetros para la consulta SQL
        :return: Lista de diccionarios con los resultados
        """
        conn = self.get_conn()
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            if parameters:
                cursor.execute(sql, parameters)
            else:
                cursor.execute(sql)

            if cursor.description:
                results = cursor.fetchall()
                return [dict(row) for row in results]
            else:
                conn.commit()
                return []

    def table_exists(self, table_name, schema='public'):
        """
        Verifica si una tabla existe en la base de datos.

        :param table_name: Nombre de la tabla a verificar
        :param schema: Esquema donde buscar la tabla (por defecto 'public')
        :return: True si la tabla existe, False en caso contrario
        """
        sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_name = %s
        )
        """
        result = self.run_query(sql, (schema, table_name))
        return result[0]['exists'] if result else False
