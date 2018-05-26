from connectors import QueryGenerator, QuerySets
import sys

query_set = int(sys.argv[1])
query_to_generate = int(sys.argv[2])

print(str(
    QueryGenerator.QueryGenerator.generate_query(QuerySets.query_sets[query_set], query_to_generate, 1, True)))
