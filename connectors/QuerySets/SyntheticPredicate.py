def get_query_set(number_of_ors):

    base = "UPDATE t.a SET a.a3 = <randIntu> WHERE a.a3 = <randIntu>"
    for x in xrange(number_of_ors):
        base = base + " OR a.a3 = <randIntu>"
    base = base + ";"
    return [ base, 100 ]