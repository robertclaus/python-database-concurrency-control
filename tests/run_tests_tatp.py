import os
import sys

time_to_run = 30
max_queries = 10000000000

# Run #1 - Vary Write %
for query_set in ['4']:
  for isolation_level in ['ru-exi','ru','s']:
    os.system("python setIsolationLevel.py "+isolation_level)
    for workers in [4]:
      use_isolation = "1 " if isolation_level == 'ru-exi' else "0 "
      argString = "python ../runners/QueryFlowTester.py {} {} {} {} {}".format(use_isolation, time_to_run, workers, max_queries, query_set)
      print(argString)
      os.system(argString)
      sys.stdout.write(", {}, {}, {} \n\n\n\n".format(isolation_level, workers, query_set))
