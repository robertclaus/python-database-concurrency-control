from connectors import QueryGeneratorConnector

from connectors.QuerySets import Synthetic
from policies.DirectPredicatePolicy import DirectPredicatePolicy

QueryGeneratorConnector.possible_query_sets = Synthetic.get_query_set(10)

q1 = QueryGeneratorConnector.QueryGeneratorConnector.generate_query(QueryGeneratorConnector.possible_query_sets, 2, DirectPredicatePolicy())
q2 = QueryGeneratorConnector.QueryGeneratorConnector.generate_query(QueryGeneratorConnector.possible_query_sets, 2, DirectPredicatePolicy())
columns_to_consider = None # {"subscriber":["sub_nbr"]}
conflicts= q1.conflicts(q2,columns_to_consider)
print("Query 1: {}\n\nQuery2: {}\n\nUsing Columns: {}\n\nConflict: {}".format(q1,q2,columns_to_consider,conflicts))