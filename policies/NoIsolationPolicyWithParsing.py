from policies.AbstractPolicy import AbstractPolicy


class NoIsolationPolicyWithParsing(AbstractPolicy):

    def __init__(self):
        pass

    # Is run on receiving the query within the client connector process
    def parse_query(self, query):
        query.parse(False)

    # Returns a list of queries to admit
    def new_query(self, query):
        return [query]

    # Returns a list of queries to admit
    def complete_query(self, query):
        return []