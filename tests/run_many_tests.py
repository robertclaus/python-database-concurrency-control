import os
import sys



for tuples_to_add in ['1000', '10000', '100000', '1000000']:
  for isolation_level in ['ru','ru-exi','rc','rr','s']:
    os.system("python IsolationLevelSetter.py "+isolation_level)
    for workers in [1, 2, 4, 8, 16, 32, 64]:
      
      print("Clearing and Generating {} Tuples".format(tuples_to_add))
      os.system("python IsolationLevelSetter.py 'd'")
      add_tuple_command = "python QueryFlowTester.py 0 1000000000 128 {} 10".format(tuples_to_add)
      os.system(add_tuple_command)
      print("Done adding Tuples")

      use_isolation = "1 " if isolation_level == 'ru-exi' else "0 "
      argString = "python QueryFlowTester.py {} {} {} {} {}".format(use_isolation, sys.argv[2], workers, sys.argv[4], sys.argv[5])
      print(argString)
      os.system(argString)
      sys.stdout.write(", {}, {}, {} \n\n\n\n".format(isolation_level, workers, tuples_to_add))
