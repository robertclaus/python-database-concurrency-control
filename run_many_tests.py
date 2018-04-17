import os
import sys

for workers in [1, 4, 8, 16, 32, 64, 128]:
  argString = ""
  argnum=0
  for arg in sys.argv:
    if argnum == 3:
      argString = argString+str(workers)+" "
    elif argnum == 0:
      argString = "python dbQueryFlowTester.py "
    else:
      argString=argString+arg+" "
    argnum=argnum+1
  os.system(argString)
