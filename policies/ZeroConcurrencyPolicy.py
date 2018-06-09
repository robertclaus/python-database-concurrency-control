from policies.AbstractPolicy import AbstractPolicy
from collections import deque


class ZeroConcurrencyPolicy(AbstractPolicy):

    def __init__(self):
        self.waiting_queries = deque()
        self.running_query = None

    def parse_query(self, query):
        pass

    def new_query(self, query):
        if not self.running_query:
            self.running_query = query
            return [query]
        else:
            self.waiting_queries.append(query)
            return []

    def complete_query(self, query):
        self.running_query = None

        if self.waiting_queries:
            waiting_query = self.waiting_queries.popleft()
            self.running_query = waiting_query
            return [waiting_query]
        else:
            return []