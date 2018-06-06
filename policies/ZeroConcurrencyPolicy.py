from policies.BasePredicatePolicy import BasePredicatePolicy
from collections import deque

class ZeroConcurrencyPolicy(BasePredicatePolicy):
    waiting_queries = deque()
    running_query = None

    @staticmethod
    def parse_query(query):
        pass

    @staticmethod
    def admit_query(query):
        if not ZeroConcurrencyPolicy.running_queries:
            ZeroConcurrencyPolicy.running_query = query
            return [query]
        else:
            ZeroConcurrencyPolicy.waiting_queries.append(query)

    @staticmethod
    def complete_query(query):
        ZeroConcurrencyPolicy.running_query = None

        if ZeroConcurrencyPolicy.waiting_queries:
            waiting_query = ZeroConcurrencyPolicy.waiting_queries.popleft()
            ZeroConcurrencyPolicy.running_query = waiting_query
            return [waiting_query]
        else:
            return []