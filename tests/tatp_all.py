import os
import sys

from IsolationLevelSetter import IsolationLevelSetter
from QueryFlowTester import QueryFlowTester

time_to_run = 30
max_queries = 10000000000

# Run #1 - Vary Write %
for query_set in [4]:
    for isolation_level in ['ru-phased', 'ru', 's']:
        dibs_policy = IsolationLevelSetter.run(isolation_level)
        for workers in [4]:

            print("QueryFlowTester.run({}, {}, {}, {}, {})".format(dibs_policy, time_to_run, workers, max_queries,
                                                                   query_set))
            QueryFlowTester.run(dibs_policy, time_to_run, workers, max_queries, [query_set])
            sys.stdout.write(", {}, {}, {} \n\n\n\n".format(isolation_level, workers, query_set))
