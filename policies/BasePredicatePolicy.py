

class BasePredicatePolicy:

    @staticmethod
    def initialize():
        pass

    # Is run on receiving the query within the client connector process
    @staticmethod
    def parse_query(query):
        pass

    # Returns a list of queries to admit
    @staticmethod
    def new_query(query):
        return [query]

    # Returns a list of queries to admit
    @staticmethod
    def complete_query(query):
        return []