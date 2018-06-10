import os
import sys

from IsolationLevelSetter import IsolationLevelSetter
from DIBSEngine import DIBSEngine

time_to_run = 30
max_queries = 10000000000

# Run #1 - Vary Write %
for query_set in [4]:
    for isolation_level in ['ru-zerocc', 'ru-phased', 'ru-directcomparison', 's']:
        dibs_policy = IsolationLevelSetter.run(isolation_level)
        for workers in [4]:

            print("DIBSEngine.run({}, {}, {}, {}, {})".format(dibs_policy, time_to_run, workers, max_queries,
                                                                   query_set))
            DIBSEngine.run(dibs_policy, time_to_run, workers, max_queries, [query_set])
            sys.stdout.write(", {}, {}, {} \n\n\n\n".format(isolation_level, workers, query_set))
