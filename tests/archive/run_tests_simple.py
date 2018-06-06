import os
import sys

time_to_run = 30
max_queries = 10000000000

# Run #1 - Vary Write %
for query_set in ['1']:
  for tuples_to_add in ['1000000']:
    for isolation_level in ['ru','ru-exi','ru','s']:
      os.system("python IsolationLevelSetter.py "+isolation_level)
      for workers in [4]:
        print("Clearing and Generating {} Tuples".format(tuples_to_add))
        #os.system("python IsolationLevelSetter.py 'd'")
        add_tuple_command = "python QueryFlowTester.py 0 1000000000 4 {} 0".format(tuples_to_add)
        #os.system(add_tuple_command)
        print("Done adding Tuples")

        use_isolation = "1 " if isolation_level == 'ru-exi' else "0 "
        argString = "python QueryFlowTester.py {} {} {} {} {}".format(use_isolation, time_to_run, workers, max_queries, query_set)
        print(argString)
        os.system(argString)
        sys.stdout.write(", {}, {}, {}, {} \n\n\n\n".format(isolation_level, workers, tuples_to_add, query_set))

