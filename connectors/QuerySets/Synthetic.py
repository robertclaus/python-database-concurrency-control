def get_query_set(readpercent):
    return [
               "SELECT a.a1 FROM t.a WHERE a.a2 = <randInt>;", readpercent,
               "UPDATE t.a SET a.a3 = <randInt> WHERE a.a2 = <randInt2>;", (100-readpercent),
            ]