from clients.AbstractClient import AbstractClient
import config

import psycopg2

class PostgresClient(AbstractClient):

    def __init__(self):
        self.connection = psycopg2.connect(database=config.POSTGRES_DB_NAME, user=config.POSTGRES_USER,
                                      password=config.POSTGRES_PASSWORD, host=config.POSTGRES_HOST,
                                      port=config.POSTGRES_PORT)
        self.cursor = self.connection.cursor()

    def execute(self, query_text):
        self.cursor.execute(query_text)
        self.connection.commit()
        return self._result_to_string()

    def _result_to_string(self):
        result = self.cursor.fetchall()
        return str(result)