import os
import sys

time_to_run = 5
max_queries = 10000000000

# Run #1 - Vary Write %
for query_set in ['1']:
  for tuples_to_add in ['100000']:
    for isolation_level in ['ru-exi','ru','s']:
      os.system("python setIsolationLevel.py "+isolation_level)
      for workers in [8]:
        print("Clearing and Generating {} Tuples".format(tuples_to_add))
        os.system("python setIsolationLevel.py 'd'")
        add_tuple_command = "python dbQueryFlowTester.py 0 1000000000 2 {} 0".format(tuples_to_add)
        os.system(add_tuple_command)
        print("Done adding Tuples")

        use_isolation = "1 " if isolation_level == 'ru-exi' else "0 "
        argString = "python dbQueryFlowTester.py {} {} {} {} {}".format(use_isolation, time_to_run, workers, max_queries, query_set)
        print(argString)
        os.system(argString)
        sys.stdout.write(", {}, {}, {}, {} \n\n\n\n".format(isolation_level, workers, tuples_to_add, query_set))

