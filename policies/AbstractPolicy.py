from abc import abstractmethod

class BasePredicatePolicy:

    @abstractmethod
    def __init__(self):
        pass

    # Is run on receiving the query within the client connector process
    @abstractmethod
    def parse_query(self, query):
        pass

    # Returns a list of queries to admit
    @abstractmethod
    def new_query(self, query):
        return [query]

    # Returns a list of queries to admit
    @abstractmethod
    def complete_query(self, query):
        return []