import os
import sys

from IsolationLevelSetter import IsolationLevelSetter
from DIBSEngine import DIBSEngine

time_to_run = 30
max_queries = 10000000000

# Run #1 - Vary Write %
for query_set in [4]:
    for isolation_level in ['ru-exi']:#, 'ru', 's']:
        IsolationLevelSetter.run(isolation_level)
        for workers in [4]:
            use_isolation = (isolation_level == 'ru-exi')

            print("DIBSEngine.run({}, {}, {}, {}, {})".format(use_isolation, time_to_run, workers, max_queries,
                                                                   query_set))
            DIBSEngine.run(use_isolation, time_to_run, workers, max_queries, [query_set])
            sys.stdout.write(", {}, {}, {} \n\n\n\n".format(isolation_level, workers, query_set))
