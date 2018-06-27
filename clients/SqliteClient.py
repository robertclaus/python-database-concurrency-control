from clients.AbstractClient import AbstractClient
import config
import sqlite3

class SqliteClient(AbstractClient):

    def __init__(self):
        self.db = sqlite3.connect('sqlitedb')
        self.cursor = self.db.cursor()
        self.cursor.execute('attach database sqlitedb as t;')
        self.db.commit()

    def execute(self, query_text):
        self.cursor.execute(query_text)
        self.db.commit()
        return self._result_to_string()

    def _result_to_string(self):
        result = self.cursor.fetchall()
        return str(result)