import os
import sys

from IsolationLevelSetter import IsolationLevelSetter
from DIBSEngine import DIBSEngine
from clients.MySQLClient import MySQLClient
from connectors.QueryGeneratorConnector import QueryGeneratorConnector
from connectors.QuerySets import TATP, TATP_Read, Synthetic5050, Insert

import config

config.MAX_SECONDS_TO_RUN = 20
config.MAX_QUERIES_TO_RUN = 1000000
config.MAX_QUERIES_IN_ENGINE = 15000

config.DEFAULT_TARGET_DEPTH = 1000
config.GENERATOR_BUNDLE_SIZE = 10
config.DEFAULT_GENERATOR_WORKER_COUNT = 2
config.MAX_GENERATORS = 20
config.PREGENERATE_ALL_QUERIES = False

# Run #1 - Vary Write %
for query_set in [Synthetic5050.query_set]:
    for workers in [5, 10, 20, 30, 40, 50]:
        for isolation_level in ['s','ru','s','ru','s','ru']:
            for synthetic_tuples in [100000]:
                print("Populating DB")
                dibs_policy = IsolationLevelSetter.run("synthetic-setup")
                config.MAX_SECONDS_TO_RUN = 1000000
                config.MAX_QUERIES_TO_RUN = synthetic_tuples
                config.NUMBER_OF_DATABASE_CLIENTS = 20
                QueryGeneratorConnector.possible_query_sets = Insert.query_set
                DIBSEngine.run(dibs_policy, MySQLClient, QueryGeneratorConnector)

                print("Running Queries")
                config.MAX_SECONDS_TO_RUN = 20
                config.MAX_QUERIES_TO_RUN = 1000000
                config.NUMBER_OF_DATABASE_CLIENTS = workers
                QueryGeneratorConnector.possible_query_sets = query_set
                dibs_policy = IsolationLevelSetter.run(isolation_level)
                QueryGeneratorConnector.last_isolation_level = isolation_level
                try:
                    DIBSEngine.run(dibs_policy, MySQLClient, QueryGeneratorConnector)
                except IOError:
                    sys.stdout.write("\n\nIO ERROR ENDED TEST\n\n")