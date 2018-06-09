from policies.AbstractPolicy import AbstractPolicy


class NoIsolationPolicy(AbstractPolicy):

    def __init__(self):
        pass

    # Is run on receiving the query within the client connector process
    def parse_query(self, query):
        pass

    # Returns a list of queries to admit
    def new_query(self, query):
        return [query]

    # Returns a list of queries to admit
    def complete_query(self, query):
        return []