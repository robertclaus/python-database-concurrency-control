# Intended to be a generic multi-threaded way of taking dbQuery objects off of one queue, running them, and putting them onto another.  The more generic this stays, the better.  It should not be aware of locks or any of that.

from abc import abstractmethod

class AbstractClient():
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def execute(self, query_text):
        # Returns the result of the query
        pass