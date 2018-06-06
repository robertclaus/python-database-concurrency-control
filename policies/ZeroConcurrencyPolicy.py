from policies.BasePredicatePolicy import BasePredicatePolicy
from collections import deque

class ZeroConcurrencyPolicy(BasePredicatePolicy):
    waiting_queries = deque()

    @staticmethod
    def parse_query(query):
        pass

    @staticmethod
    def admit_query(query):
        if len(ZeroConcurrencyPolicy.waiting_queries)==0:
            return [query]
        else:
            ZeroConcurrencyPolicy.waiting_queries.append(query)

    @staticmethod
    def complete_query(query):
        return [ZeroConcurrencyPolicy.waiting_queries.popleft()]