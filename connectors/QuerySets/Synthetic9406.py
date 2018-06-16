query_set =  [
               "SELECT a.a1 FROM t.a WHERE a.a2 > 100000;", 47,
               "SELECT a.a1 FROM t.a WHERE a.a2 < 100000;", 47,
               "UPDATE t.a SET a.a3 = <randInt> WHERE a.a2 < 100000;", 3,
               "UPDATE t.a SET a.a3 = <randInt> WHERE a.a2 > 100000;", 3,
               ]