import os
import sys

for isolation_level in ['ru','ru-exi','rc','rr','s']:
  os.system("python setIsolationLevel.py "+isolation_level)
  for workers in [1, 2, 4, 8, 10, 12]:
    argString = ""
    argnum=0
    for arg in sys.argv:
      if argnum == 3:
        argString = argString+str(workers)+" "
      elif argnum == 0:
        argString = "python dbQueryFlowTester.py "
      elif argnum == 1: # Isolation Mode 0 for all but read uncomitted - external
        if isolation_level == 'ru-exi':
          argString = argString + "1 "
        else:
          argString = argString + "0 "
      else:
        argString=argString+arg+" "
      argnum=argnum+1
    print(argString)
    os.system(argString)
    print("Ran in Isolation Mode {} with {} workers.\n\n\n\n".format(isolation_level, workers))
