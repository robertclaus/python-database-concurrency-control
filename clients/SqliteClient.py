import time

from clients.AbstractClient import AbstractClient
import config
import sqlite3

class SqliteClient(AbstractClient):
    initialization_query = None

    def __init__(self):
        self.db = sqlite3.connect('sqlitedb', timeout=100)
        self.cursor = self.db.cursor()
        self.cursor.execute('PRAGMA journal_mode=WAL;')
        if SqliteClient.initialization_query:
            self.cursor.execute(SqliteClient.initialization_query)
        self.cursor.execute('PRAGMA wal_autocheckpoint=1000;')
        self.cursor.execute('attach database sqlitedb as t;')
        self.db.commit()

    def execute(self, query_text):

        for i in xrange(1,1000):
            try:
                self.cursor = self.db.cursor()
                self.cursor.execute(query_text)
                self.db.commit()
                return self._result_to_string()
            except sqlite3.OperationalError as e:
                if not 'database is locked' in str(e):
                    raise e
                time.sleep(.01)
        raise sqlite3.OperationalError('database is locked')

    def _result_to_string(self):
        result = self.cursor.fetchall()
        return str(result)