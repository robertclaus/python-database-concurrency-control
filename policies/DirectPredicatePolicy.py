from policies.BasePredicatePolicy import BasePredicatePolicy
from collections import deque


class DirectPredicatePolicy(BasePredicatePolicy):
    sidetracked_queries = deque()

    # Is run on receiving the query within the client connector process
    @staticmethod
    def parse_query(query):
        query.parse()

    # Returns a list of queries to admit
    @staticmethod
    def new_query(query):
        return [query]

    # Returns a list of queries to admit
    @staticmethod
    def complete_query(query):
        return []