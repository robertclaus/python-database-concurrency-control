from connectors import QueryGenerator, QuerySets
import sys

query_set = int(sys.argv[1])
query_to_generate = int(sys.argv[2])

q1 = QueryGenerator.QueryGenerator.generate_query(QuerySets.query_sets[query_set], query_to_generate, 1, True)
q2 = QueryGenerator.QueryGenerator.generate_query(QuerySets.query_sets[query_set], query_to_generate, 1, True)
columns_to_consider = {"subscriber":["sub_nbr"]}
conflicts= q1.conflicts(q2,columns_to_consider)
print("Query 1: {}\n\nQuery2: {}\n\nUsing Columns: {}\n\nConflict: {}".format(q1,q2,columns_to_consider,conflicts))