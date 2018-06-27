import os
import sys

from tests.IsolationLevelSetter import IsolationLevelSetter
from DIBSEngine import DIBSEngine
from clients.MySQLClient import MySQLClient
from clients.SqliteClient import SqliteClient
from clients.PostgresClient import PostgresClient

from connectors.QueryGeneratorConnector import QueryGeneratorConnector
from connectors.QuerySets import Synthetic
from policies.PhasedPolicy import PhasedPolicy

import config

config.MAX_SECONDS_TO_RUN = 20
config.MAX_QUERIES_TO_RUN = 10000
config.MAX_QUERIES_IN_ENGINE = 300

config.DEFAULT_TARGET_DEPTH = 1000
config.GENERATOR_BUNDLE_SIZE = 6
config.DEFAULT_GENERATOR_WORKER_COUNT = 2
config.MAX_GENERATORS = 25
config.PREGENERATE_ALL_QUERIES = False

for readpercent in [10, 30, 50, 70, 90]:
    QueryGeneratorConnector.possible_query_sets = Synthetic.get_query_set(readpercent)
    for workers in [10]:
        config.NUMBER_OF_DATABASE_CLIENTS = workers
        for isolation_level in [ 'ru-directcomparison', 'ru', 'ru-zerocc', 's']:
            for dbclient in [PostgresClient, SqliteClient, MySQLClient]:
                for synthetic_tuples in [100000]:
                    print("Populating DB")
                    IsolationLevelSetter.setup(synthetic_tuples, dbclient)

                    print("Running Queries")
                    dibs_policy = IsolationLevelSetter.run(isolation_level)
                    QueryGeneratorConnector.last_isolation_level = isolation_level
                    PhasedPolicy.lock_combinations = [['a.a3']]

                    try:
                        DIBSEngine.run(dibs_policy, dbclient, QueryGeneratorConnector)
                    except IOError:
                        sys.stdout.write("\n\nIO ERROR ENDED TEST\n\n")