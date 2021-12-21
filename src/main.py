from Classes.Sqlite import Sqlite
from Classes.Mysql import MysqlDriver

if __name__ == '__main__':
    # Create SQLITE tables
    # Sqlite.create_tables()

    # MysqlDriver.create_connection()
    # Sqlite.check_connection()

    conn_sqlite = Sqlite.create_connection()

    with conn_sqlite:
        dados = ["conceitual_data", "data_classification", "table_schema", "table_name", "column_name", "date_type", "column_type"]
        sqlite = Sqlite()
        sqlite.insert_into_base(conn_sqlite, dados)

