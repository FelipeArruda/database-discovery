import os
import re
import sqlite3
import dotenv
from datetime import datetime
from sqlalchemy import create_engine
from sqlite3 import Error

# load .env file
dotenv.load_dotenv(dotenv.find_dotenv())


class Sqlite:

    def __init__(self):
        # self.table_schema = table_schema
        # self.table_name = table_name
        # self.column_name = column_name
        # self.data_type = data_type
        # self.column_type = column_type
        self.has_sensitive = "N"
        self.is_empty_table = "N"
        self.regexp_ip = "N"
        self.regex_phone = "N"
        self.regexp_email = "N"
        self.regex_address = "N"
        self.regex_links = "N"
        self.regex_social_media = "N"
        self.regex_cpf = "N"
        self.regex_credit_card = "N"
        self.regex_name = "N"
        self.executed = "N"

    @staticmethod
    def create_connection():
        conn = None
        try:
            conn = sqlite3.connect("../datasets/base_sqlite.db")
        except Error as e:
            print(e)
        return conn

    @staticmethod
    def check_connection():
        global sqlite_connection
        try:
            sqlite_connection = sqlite3.connect(os.getenv("SQLITE_DATABASE"))
            cursor = sqlite_connection.cursor()
            print("Database Connected")

            sqlite_select_query = "select sqlite_version();"
            cursor.execute(sqlite_select_query)
            record = cursor.fetchall()
            print("SQLite Database Version is: ", record)
            cursor.close()
            return True
        except sqlite3.Error as error:
            print("Error while connecting to sqlite", error)
            return False
        finally:
            if sqlite_connection:
                sqlite_connection.close()
                print("The SQLite connection is closed")
                return False

    @staticmethod
    def create_tables():

        engine = create_engine(os.getenv("SQLITE_PATH") + os.getenv("SQLITE_DATABASE"))

        # table: name
        engine.execute('CREATE TABLE IF NOT EXISTS "names" ('
                       'id integer PRIMARY KEY AUTOINCREMENT,'
                       'name VARCHAR (500) '
                       ');')

        # table: terms
        engine.execute('CREATE TABLE IF NOT EXISTS "terms" ('
                       'id integer PRIMARY KEY AUTOINCREMENT,'
                       'term VARCHAR (500) '
                       ');')

        # table: configs
        engine.execute('CREATE TABLE IF NOT EXISTS "configs" ('
                       'id integer PRIMARY KEY AUTOINCREMENT,'
                       'param VARCHAR (100), '
                       'param_valor VARCHAR (500), '
                       'param_system VARCHAR (500), '
                       'start_date DATE, '
                       'end_date DATE '
                       ');')

        # table: base
        engine.execute('CREATE TABLE IF NOT EXISTS "base" ('
                       'id integer PRIMARY KEY AUTOINCREMENT,'
                       'server_name VARCHAR (500), '
                       'database VARCHAR (500), '
                       'conceitual_data VARCHAR (500), '
                       'data_classification VARCHAR (500), '
                       'table_schema VARCHAR (500), '
                       'table_name VARCHAR (500), '
                       'column_name VARCHAR (500), '
                       'date_type VARCHAR (500), '
                       'column_type VARCHAR (500), '
                       'sql VARCHAR (500), '
                       'has_sensitive VARCHAR (500), '
                       'is_empty_table VARCHAR (500), '
                       'obs VARCHAR (500), '
                       'classification_source VARCHAR (500), '
                       'regex_ip char(1), '
                       'regex_phone char(1), '
                       'regex_email char(1), '
                       'regex_address char(1), '
                       'regex_links char(1), '
                       'regex_social_media char(1), '
                       'regex_cpf char(1), '
                       'regex_credit_card char(1), '
                       'regex_name char(1), '
                       'date datetime, '
                       'executed char(1) '
                       ');')

    def insert_into_base(self, conn, data):
        sql = "insert into base (server_name, database, conceitual_data, data_classification, table_schema, table_name, column_name, " \
              "date_type, column_type, sql, has_sensitive, is_empty_table, regex_ip, regex_phone, regex_email, " \
              "regex_address, regex_links, regex_social_media, regex_cpf, regex_credit_card, regex_name, executed, date) values " \
              "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?,'" + self.has_sensitive + "', '" + self.is_empty_table + "', '" + self.regexp_ip + \
              "', '" + self.regex_phone + "', '" + self.regexp_email + "', '" + self.regex_address + "', '" + self.regex_links + \
              "', '" + self.regex_social_media + "', '" + self.regex_cpf + "', '" + self.regex_credit_card + "', '" + self.regex_name + "', '" + self.executed + \
              "', '" + datetime.today().strftime('%d-%m-%Y %H:%M:%S') + "'); "

        cursor = conn.cursor()
        cursor.execute(sql, data)
        conn.commit()
        return cursor.lastrowid

    def replaceNth(s, source, target, n):
        inds = [i for i in range(len(s) - len(source) + 1) if s[i:i + len(source)] == source]
        if len(inds) < n:
            return  # or maybe raise an error
        s = list(s)  # can't assign to string slices. So, let's listify
        s[inds[n - 1]:inds[n - 1] + len(
            source)] = target  # do n-1 because we start from the first occurrence of the string, not the 0-th
        return ''.join(s)

    def select_config_by_param(conn, param, campo, tabela):
        cur = conn.cursor()
        cur.execute("SELECT * FROM configs WHERE param=?", (param,))
        rows = cur.fetchall()
        r1 = Sqlite.replaceNth(rows[0][2], '?', campo, 1)
        r2 = Sqlite.replaceNth(r1, '?', tabela, 1)
        return r2