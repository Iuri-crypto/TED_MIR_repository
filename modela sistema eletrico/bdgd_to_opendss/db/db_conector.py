import psycopg2
from psycopg2 import OperationalError

def get_connection(host: str, port: int, dbname: str, user: str, password: str):
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        print(f"Conectado ao banco {dbname} com sucesso!")
        return conn
    except OperationalError as e:
        print(f"Erro na conexão com o banco {dbname}: {e}")
        return None
