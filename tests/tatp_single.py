import os
import sys

from IsolationLevelSetter import IsolationLevelSetter
from DIBSEngine import DIBSEngine
from clients.MySQLClient import MySQLClient
from connectors.QueryGeneratorConnector import QueryGeneratorConnector
from connectors.QuerySets import query_sets

import config

config.MAX_SECONDS_TO_RUN = 120
config.MAX_QUERIES_TO_RUN = 25000

# Run #1 - Vary Write %
for query_set in [4]:
    QueryGeneratorConnector.possible_query_sets = query_sets[query_set]
    for workers in [16]:
        config.NUMBER_OF_DATABASE_CLIENTS = workers
        for isolation_level in ['ru-phased']:#,'ru-phased-integrated','ru', 'ru-p']:
            dibs_policy = IsolationLevelSetter.run(isolation_level)
            QueryGeneratorConnector.last_isolation_level = isolation_level
            try:
                DIBSEngine.run(dibs_policy, MySQLClient, QueryGeneratorConnector)
            except IOError:
                sys.stdout.write("\n\nIO ERROR ENDED TEST\n\n")
