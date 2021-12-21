from Classes.Sqlite import Sqlite
from Classes.Mysql import MysqlDriver
from Classes.Postgres import Postgres

server_name = "a1-olden-q-prt1.host.intranet"

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

    # Postgres
    db_pgsql = Postgres("ctl_farruda", "7F7Mea", server_name , "5432", "postgres")
    cursor_pgsql = db_pgsql.conn.cursor()
    cursor_pgsql.execute("SELECT datname FROM pg_database where datname not in ('template0', 'template1', 'postgres')")
    rec_database = cursor_pgsql.fetchall()

    for rd in rec_database:
        # busca os databases
        db_pgsql = Postgres("ctl_farruda", "7F7Mea", "a1-olden-q-prt1.host.intranet", "5432", rd[0])
        cursor_pgsql = db_pgsql.conn.cursor()
        cursor_pgsql.execute(
            "SELECT nspname FROM pg_catalog.pg_namespace where (nspname not like 'pg_%' and nspname not like "
            "'information_schema%'); ")
        rec_schemas = cursor_pgsql.fetchall()

        # busca os schemas
        for rs in rec_schemas:
            # busca as tabelas
            cursor_pgsql = db_pgsql.conn.cursor()
            cursor_pgsql.execute(
                "SELECT n.nspname as esquema, c.relname as tabela, a.attname as campo, format_type(t.oid, null) as "
                "tipo_de_dado FROM pg_namespace n, pg_class c, pg_attribute a, pg_type t WHERE n.oid = c.relnamespace "
                "and c.relkind = 'r' and a.attnum > 0 and n.nspname not like 'pg_%' and n.nspname != "
                "'information_schema' and a.attnum > 0    and a.attrelid = c.oid   and a.atttypid = t.oid and "
                "n.nspname = '" + rs[0] + "';")

            rec_tables = cursor_pgsql.fetchall()
            for rt in rec_tables:
                with conn_sqlite:
                    dados = [server_name, rd[0], "NI", "NI", rt[0], rt[1], rt[2], "NI", rt[3]]
                    sqlite = Sqlite()
                    sqlite.insert_into_base(conn_sqlite, dados)
                # print(*rt)

        db_pgsql.close_connection()



