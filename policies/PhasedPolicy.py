from policies.BasePredicatePolicy import BasePredicatePolicy


class PhasedPolicy(BasePredicatePolicy):

    @staticmethod
    def initialize():
        pass


    @staticmethod
    def parse_query(query):
        query.parse(True)

    @staticmethod
    def new_query(query):
        return True
