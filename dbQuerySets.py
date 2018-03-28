query_sets = [
              [
               "SELECT a1 FROM a WHERE a2=<randInt>;",
               "UPDATE a SET a3=<randInt> WHERE a2=<randInt2>;",
               ],
              
              [
               "UPDATE a SET a3=<randInt>;"
               ],
              
              [
               "SELECT a3 FROM a WHERE a2=<randInt>;",
               ],
              
              [
               "UPDATE a SET a3=<randInt> WHERE a2=<randInt2>;"
               ],
              
              [
               "SELECT a2 FROM a WHERE a2=<randInt>;",
               ],
              
              [
               "INSERT INTO a (a1, a2, a3) VALUES (<randInt>, <randInt2>, <randInt3>);"
               ],
              
              [
               "SELECT a3 FROM a WHERE a1=<randInt>;",
               ],
              
              
]
