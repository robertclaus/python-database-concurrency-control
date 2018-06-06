from policies.BasePredicatePolicy import BasePredicatePolicy
from collections import deque

class ZeroConcurrencyPolicy(BasePredicatePolicy):
    waiting_queries = deque()
    running_queries = []

    @staticmethod
    def parse_query(query):
        pass

    @staticmethod
    def admit_query(query):
        if not ZeroConcurrencyPolicy.running_queries:
            ZeroConcurrencyPolicy.running_queries.append(query)
            return [query]
        else:
            ZeroConcurrencyPolicy.waiting_queries.append(query)

    @staticmethod
    def complete_query(query):
        ZeroConcurrencyPolicy.running_queries.remove(query)

        if ZeroConcurrencyPolicy.waiting_queries:
            query = ZeroConcurrencyPolicy.waiting_queries.popleft()
            ZeroConcurrencyPolicy.running_queries.append(query)
            return [query]
        else:
            return []