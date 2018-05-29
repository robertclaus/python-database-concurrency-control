from connectors import QueryGenerator, QuerySets
import sys

query_set = int(sys.argv[1])
query_to_generate = int(sys.argv[2])

q1 = QueryGenerator.QueryGenerator.generate_query(QuerySets.query_sets[query_set], query_to_generate, 1, True)
q2 = QueryGenerator.QueryGenerator.generate_query(QuerySets.query_sets[query_set], query_to_generate, 1, True)
conflicts= q1.conflicts(q2)
print("Query 1: {}\n\nQuery2: {}\n\nConflict: {}".format(q1,q2,conflicts))