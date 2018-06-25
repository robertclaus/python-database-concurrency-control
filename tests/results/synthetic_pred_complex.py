import os
import sys

from tests.IsolationLevelSetter import IsolationLevelSetter
from DIBSEngine import DIBSEngine
from clients.MySQLClient import MySQLClient
from connectors.QueryGeneratorConnector import QueryGeneratorConnector
from connectors.QuerySets import SyntheticPredicate
from policies.PhasedPolicy import PhasedPolicy

import config

config.MAX_SECONDS_TO_RUN = 20000
config.MAX_QUERIES_TO_RUN = 1000
config.MAX_QUERIES_IN_ENGINE = 1000

config.DEFAULT_TARGET_DEPTH = 1000
config.GENERATOR_BUNDLE_SIZE = 10
config.DEFAULT_GENERATOR_WORKER_COUNT = 2
config.MAX_GENERATORS = 20
config.PREGENERATE_ALL_QUERIES = False

for pred_count in [1, 10, 100, 1000]:
    QueryGeneratorConnector.possible_query_sets = SyntheticPredicate.get_query_set(pred_count)
    for workers in [20]:
        config.NUMBER_OF_DATABASE_CLIENTS = workers
        for isolation_level in [ 'ru-directcomparison', 'ru', 'ru-zerocc', 's']:
            for synthetic_tuples in [10000]:
                print("Populating DB")
                IsolationLevelSetter.setup(synthetic_tuples)

                print("Running Queries")
                dibs_policy = IsolationLevelSetter.run(isolation_level)
                QueryGeneratorConnector.last_isolation_level = isolation_level
                PhasedPolicy.lock_combinations = [['a.a3']]

                try:
                    DIBSEngine.run(dibs_policy, MySQLClient, QueryGeneratorConnector)
                except IOError:
                    sys.stdout.write("\n\nIO ERROR ENDED TEST\n\n")