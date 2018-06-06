from policies.BasePredicatePolicy import BasePredicatePolicy


class PhasedPolicy(BasePredicatePolicy):
    @staticmethod
    def parse_query(query):
        query.parse()

    @staticmethod
    def new_query(query):
        return True
