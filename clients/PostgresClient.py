from psycopg2._psycopg import ProgrammingError

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
        try:
            self.cursor.execute(query_text)
            self.connection.commit()
            return self._result_to_string()
        except ProgrammingError as e:
            if "no results to fetch" in str(e):
                return ""
            else:
                raise e


    def _result_to_string(self):
        result = self.cursor.fetchall()
        return str(result)