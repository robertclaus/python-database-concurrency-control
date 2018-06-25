from connectors.QueryGeneratorConnector import QueryGeneratorConnector
from connectors.QuerySets import SyntheticPredicate
from policies.DirectPredicatePolicy import DirectPredicatePolicy
print(str(
    QueryGeneratorConnector.generate_query(SyntheticPredicate.get_query_set(5), 0, DirectPredicatePolicy())))
