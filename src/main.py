import os
import time
import dotenv
from Classes.Sqlite import Sqlite
from Classes.Mysql import MysqlDriver
from Classes.Postgres import Postgres

# load .env file
dotenv.load_dotenv(dotenv.find_dotenv())

start_time = time.time()

if __name__ == '__main__':
    # Create SQLITE tables
    # Sqlite.create_tables()

    # MysqlDriver.create_connection()
    # Sqlite.check_connection()

    # Sqlite
    conn_sqlite = Sqlite.create_connection()
    #
    # with conn_sqlite:
    #     dados = ["conceitual_data", "data_classification", "table_schema", "table_name", "column_name", "date_type", "column_type"]
    #     sqlite = Sqlite()
    #     sqlite.insert_into_base(conn_sqlite, dados)
    # with conn_sqlite:
    #   db = Sqlite.select_config_by_param(conn_sqlite, 'select.postgres', 'campo', 'tabela')
    #   print(db)
    # a = Sqlite.replaceNth(db, '?', 'campo', 1)
    # b = Sqlite.replaceNth(a, '?', 'tabela', 1)

    # print(b)

    # # Postgres
    db_pgsql = Postgres(os.getenv("PGS_USER"), os.getenv("PGS_PASSWORD"), os.getenv("SERVER_NAME"), os.getenv("PGS_PORT"), os.getenv("PGS_DATABASE"))
    cursor_pgsql = db_pgsql.conn.cursor()
    # cursor_pgsql.execute("SELECT datname FROM pg_database where datname not in ('template0', 'template1', 'postgres')")
    # rec_database = cursor_pgsql.fetchall()
    #
    # for rd in rec_database:
    #     # busca os databases
    #     db_pgsql = Postgres(os.getenv("PGS_USER"), os.getenv("PGS_PASSWORD"), os.getenv("SERVER_NAME"), os.getenv("PGS_PORT"), rd[0])
    #     cursor_pgsql = db_pgsql.conn.cursor()
    #     cursor_pgsql.execute(
    #         "SELECT nspname FROM pg_catalog.pg_namespace where (nspname not like 'pg_%' and nspname not like "
    #         "'information_schema%'); ")
    #     rec_schemas = cursor_pgsql.fetchall()
    #
    #     # busca os schemas
    #     for rs in rec_schemas:
    #         # busca as tabelas
    #         cursor_pgsql = db_pgsql.conn.cursor()
    #         cursor_pgsql.execute(
    #             "SELECT n.nspname as esquema, c.relname as tabela, a.attname as campo, format_type(t.oid, null) as "
    #             "tipo_de_dado FROM pg_namespace n, pg_class c, pg_attribute a, pg_type t WHERE n.oid = c.relnamespace "
    #             "and c.relkind = 'r' and a.attnum > 0 and n.nspname not like 'pg_%' and n.nspname != "
    #             "'information_schema' and a.attnum > 0    and a.attrelid = c.oid   and a.atttypid = t.oid and "
    #             "n.nspname = '" + rs[0] + "';")
    #
    #         rec_tables = cursor_pgsql.fetchall()
    #         for rt in rec_tables:
    #             with conn_sqlite:
    #                 dados = [os.getenv("SERVER_NAME"), rd[0], "NI", "NI", rt[0], rt[1], rt[2], "NI", rt[3], Sqlite.select_config_by_param(conn_sqlite, 'select.postgres', rt[2], rt[0] + "." + rt[1]), os.getenv("BASE_INPUT")]
    #                 sqlite = Sqlite()
    #                 sqlite.insert_into_base(conn_sqlite, dados)

    # busca as linhas não processadas do sqlite
    with conn_sqlite:
        db = Sqlite.select_all_data(conn_sqlite, "PGSQL")
        for r_sqlite in db:
            db_pgsql = Postgres(os.getenv("PGS_USER"), os.getenv("PGS_PASSWORD"), os.getenv("SERVER_NAME"),
                                os.getenv("PGS_PORT"), r_sqlite[0])
            cursor_pgsql = db_pgsql.conn.cursor()
            cursor_pgsql.execute(r_sqlite[1])
            r_data = cursor_pgsql.fetchall()

            # aplicar as regras de validação
            for r in r_data:
                print(r[0])
            # print(r_sqlite[0] + ":" + r_sqlite[1])

        db_pgsql.close_connection()

print("Execution: %s seconds." % (time.time() - start_time))