import sqlite3


class Sqlite:
    def __init__(self, val):
        self.val = val

    @staticmethod
    def create_connection():
        conn = sqlite3.connect("base.db")
        print("Connect to SQLITE.")
        return conn
