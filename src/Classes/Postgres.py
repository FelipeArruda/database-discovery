import psycopg2
from psycopg2 import Error


class Postgres:
    def __init__(self, user, password, host, port, database):
        try:
            self.conn = psycopg2.connect(
                user=user,
                password=password,
                host=host,
                port=port,
                database=database)
            self.cursor = self.conn.cursor()
        except (Exception, Error) as error:
            print("Erro ao conectar ao Postgres: ", error)

    def close_connection(self):
        self.cursor.close()
        self.conn.close()

    def query(self, query):
        self.cursor.execute(query)

