import os
import sys

time_to_run = 60
max_queries = 10000000000

# Run #1 - Vary Write %
for query_set in ['1','2','3','4','5','6','7']:
  for tuples_to_add in ['100000']:
    for isolation_level in ['ru-exi','ru','rc','rr','s']:
      os.system("python setIsolationLevel.py "+isolation_level)
      for workers in [16]:
        print("Clearing and Generating {} Tuples".format(tuples_to_add))
        os.system("python setIsolationLevel.py 'd'")
        add_tuple_command = "python QueryFlowTester.py 0 1000000000 128 {} 0".format(tuples_to_add)
        os.system(add_tuple_command)
        print("Done adding Tuples")

        use_isolation = "1 " if isolation_level == 'ru-exi' else "0 "
        argString = "python QueryFlowTester.py {} {} {} {} {}".format(use_isolation, time_to_run, workers, max_queries, query_set)
        print(argString)
        os.system(argString)
        sys.stdout.write(", {}, {}, {}, {} \n\n\n\n".format(isolation_level, workers, tuples_to_add, query_set))

#Test 2 - Vary Database Workload - Slightly more conflicty.
for query_set in ['8']:
  for tuples_to_add in ['10000','50000','100000','200000','300000','400000','600000','800000','1000000']:
    for isolation_level in ['ru-exi','ru','rc','rr','s']:
      os.system("python setIsolationLevel.py "+isolation_level)
      for workers in [16]:
        print("Clearing and Generating {} Tuples".format(tuples_to_add))
        os.system("python setIsolationLevel.py 'd'")
        add_tuple_command = "python QueryFlowTester.py 0 1000000000 128 {} 0".format(tuples_to_add)
        os.system(add_tuple_command)
        print("Done adding Tuples")
        
        use_isolation = "1 " if isolation_level == 'ru-exi' else "0 "
        argString = "python QueryFlowTester.py {} {} {} {} {}".format(use_isolation, time_to_run, workers, max_queries, query_set)
        print(argString)
        os.system(argString)
        sys.stdout.write(", {}, {}, {}, {} \n\n\n\n".format(isolation_level, workers, tuples_to_add, query_set))
