from psycopg2._psycopg import ProgrammingError

from clients.AbstractClient import AbstractClient
import config

import psycopg2

class PostgresClient(AbstractClient):
    initialization_query = None

    def __init__(self):
        self.connection = psycopg2.connect(database=config.POSTGRES_DB_NAME, user=config.POSTGRES_USER,
                                      password=config.POSTGRES_PASSWORD, host=config.POSTGRES_HOST,
                                      port=config.POSTGRES_PORT)
        self.cursor = self.connection.cursor()
        if not PostgresClient.initialization_query is None:
            self.cursor.execute(PostgresClient.initialization_query)
            self.connection.commit()

    def execute(self, query_text):
        for i in xrange(1, 1000):
            try:
                self.cursor = self.connection.cursor()
                self.cursor.execute(query_text)
                self.connection.commit()
                return self._result_to_string()
            except ProgrammingError as e:
                if "no results to fetch" in str(e):
                    return ""
                elif "The transaction might succeed if retried." in str(e):
                    self.cursor.close()
                else:
                    raise e
        raise psycopg2.OperationalError('Retried 1000 times!!!')


    def _result_to_string(self):
        result = self.cursor.fetchall()
        return str(result)