import os
import sys

from IsolationLevelSetter import IsolationLevelSetter
from QueryFlowTester import QueryFlowTester
from clients.MySQLClient import MySQLClient
from connectors.QueryGeneratorConnector import QueryGeneratorConnector
from connectors.QuerySets import query_sets

time_to_run = 30
max_queries = 25000

# Run #1 - Vary Write %
for query_set in [4]:
    QueryGeneratorConnector.possible_query_sets = query_sets[query_set]
    for workers in [2]:#, 4, 6, 8, 10, 12, 14, 16]:
        for isolation_level in ['ru-phased', 'ru-directcomparison', 'ru-zerocc', 'ru', 's']:
            dibs_policy = IsolationLevelSetter.run(isolation_level)

            print("QueryFlowTester.run({}, {}, {}, {})".format(dibs_policy, time_to_run, workers, max_queries))
            try:
                QueryFlowTester.run(dibs_policy, MySQLClient, QueryGeneratorConnector, time_to_run, workers, max_queries)
            except IOError:
                sys.stdout.write("\n\nIO ERROR ENDED TEST\n\n")

            sys.stdout.write(", {}, {}, {} \n\n\n\n".format(isolation_level, workers, query_set))
