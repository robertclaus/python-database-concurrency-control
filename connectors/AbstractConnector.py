
from abc import abstractmethod

class AbstractConnector:

    @abstractmethod
    def __init__(self, policy):
        pass

    @abstractmethod
    def end_processes(self):
        pass

    @abstractmethod
    def next_queries(self):
        pass

    @abstractmethod
    def complete_query(self, query):
        pass