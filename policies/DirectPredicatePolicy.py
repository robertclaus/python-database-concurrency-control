from policies.BasePredicatePolicy import BasePredicatePolicy
from collections import deque


class DirectPredicatePolicy(BasePredicatePolicy):
    sidetracked_queries = deque()
    running_queries = deque()

    # Is run on receiving the query within the client connector process
    @staticmethod
    def parse_query(query):
        query.parse()

    # Returns a list of queries to admit
    @staticmethod
    def new_query(query):
        # Check for conflicts
        for running_query in DirectPredicatePolicy.running_queries:
            if query.conflicts(running_query, None):
                DirectPredicatePolicy.sidetracked_queries.append(query)
                return []

        # No conflicts
        DirectPredicatePolicy.running_queries.append(query)
        return [query]

    # Returns a list of queries to admit
    @staticmethod
    def complete_query(query):
        DirectPredicatePolicy.running_queries.remove(query)

        queries_to_admit = []
        for waiting_query in DirectPredicatePolicy.sidetracked_queries:
            can_admit_waiting_query = True
            for running_query in DirectPredicatePolicy.running_queries:
                if waiting_query.conflicts(running_query, None):
                    can_admit_waiting_query = False

            if can_admit_waiting_query:
                DirectPredicatePolicy.running_queries.append(waiting_query)
                queries_to_admit.append(waiting_query)

        return queries_to_admit
