from clients.AbstractConnector import AbstractConnector
from config import config

import MySQLdb

class MySQLConnector(AbstractConnector):

    def __init__(self):
        self.connection = MySQLdb.connect(host=config.MYSQL_HOST,
                                          user=config.MYSQL_USER,
                                          passwd=config.MYSQL_PASSWORD,
                                          db=config.MYSQL_DB_NAME)
        self.cursor = self.connection.cursor()

    def execute(self, query_text):
        self.cursor.execute(query_text)
        self.connection.commit()
