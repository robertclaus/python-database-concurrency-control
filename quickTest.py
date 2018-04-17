import dbQueryGenerator
import dbQuerySets
import sys

query_set = int(sys.argv[1])
query_to_generate = int(sys.argv[2])

dbQueryGenerator.dbQueryGenerator.generate_query(dbQuerySets.query_sets[query_set],query_to_generate,1,True).print_locks()
