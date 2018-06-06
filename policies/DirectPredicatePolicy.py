import time

from policies.BasePredicatePolicy import BasePredicatePolicy
from collections import deque


class DirectPredicatePolicy(BasePredicatePolicy):
    sidetracked_queries = deque()
    running_queries = deque()
    max_sidetrack_wait = 1

    # Is run on receiving the query within the client connector process
    @staticmethod
    def parse_query(query):
        query.parse(False)

    # Returns a list of queries to admit
    @staticmethod
    def new_query(query):
        # Check for conflicts
        for running_query in DirectPredicatePolicy.running_queries:
            if query.conflicts(running_query, None):
                DirectPredicatePolicy.sidetracked_queries.append(query)
                return []

        # If we could admit right now, should we to be fair to sidetrack?
        if DirectPredicatePolicy.sidetracked_queries:
            next_sidetracked_query = DirectPredicatePolicy.sidetracked_queries[0]

            if next_sidetracked_query.time_since_admit() < DirectPredicatePolicy.max_sidetrack_wait:
                # No conflicts and fair to admit ahead of the sidetrack
                DirectPredicatePolicy.running_queries.append(query)
                return [query]

            if not query.conflicts(next_sidetracked_query, None):
                DirectPredicatePolicy.running_queries.append(query)
                return [query]
        else:
            # Nothing in sidetrack so can admit
            DirectPredicatePolicy.running_queries.append(query)
            return [query]

        return []


    # Returns a list of queries to admit
    @staticmethod
    def complete_query(query):
        DirectPredicatePolicy.running_queries.remove(query)

        queries_to_admit = []
        if DirectPredicatePolicy.sidetracked_queries:
            next_sidetracked_query = DirectPredicatePolicy.sidetracked_queries[0]

            if next_sidetracked_query.time_since_admit > DirectPredicatePolicy.max_sidetrack_wait:
                queries_to_consider = [next_sidetracked_query]
            else:
                queries_to_consider = DirectPredicatePolicy.sidetracked_queries

            for waiting_query in queries_to_consider:
                can_admit_waiting_query = True
                for running_query in DirectPredicatePolicy.running_queries:
                    if waiting_query.conflicts(running_query, None):
                        can_admit_waiting_query = False

                if can_admit_waiting_query:
                    DirectPredicatePolicy.running_queries.append(waiting_query)
                    queries_to_admit.append(waiting_query)

            for query in queries_to_admit:
                DirectPredicatePolicy.sidetracked_queries.remove(query)

        return queries_to_admit
