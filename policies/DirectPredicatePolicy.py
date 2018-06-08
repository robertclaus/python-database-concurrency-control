import time

from policies.AbstractPolicy import AbstractPolicy
from collections import deque


class DirectPredicatePolicy(AbstractPolicy):

    def __init__(self):
        self.sidetracked_queries = deque()
        self.running_queries = deque()
        self.max_sidetrack_wait = 1

    # Is run on receiving the query within the client connector process
    def parse_query(self, query):
        query.parse(False)

    # Returns a list of queries to admit
    def new_query(self, query):
        # Check for conflicts
        for running_query in self.running_queries:
            if query.conflicts(running_query, None):
                self.sidetracked_queries.append(query)
                return []

        # If we could admit right now, should we to be fair to sidetrack?
        if self.sidetracked_queries:
            next_sidetracked_query = self.sidetracked_queries[0]

            if next_sidetracked_query.time_since_admit() < self.max_sidetrack_wait:
                # No conflicts and fair to admit ahead of the sidetrack
                self.running_queries.append(query)
                return [query]

            if not query.conflicts(next_sidetracked_query, None):
                self.running_queries.append(query)
                return [query]
        else:
            # Nothing in sidetrack so can admit
            self.running_queries.append(query)
            return [query]

        return []


    # Returns a list of queries to admit
    def complete_query(self, query):
        self.running_queries.remove(query)

        queries_to_admit = []
        if self.sidetracked_queries:
            next_sidetracked_query = self.sidetracked_queries[0]

            if next_sidetracked_query.time_since_admit > self.max_sidetrack_wait:
                queries_to_consider = [next_sidetracked_query]
            else:
                queries_to_consider = self.sidetracked_queries

            for waiting_query in queries_to_consider:
                can_admit_waiting_query = True
                for running_query in self.running_queries:
                    if waiting_query.conflicts(running_query, None):
                        can_admit_waiting_query = False

                if can_admit_waiting_query:
                    self.running_queries.append(waiting_query)
                    queries_to_admit.append(waiting_query)

            for query in queries_to_admit:
                self.sidetracked_queries.remove(query)

        return queries_to_admit
