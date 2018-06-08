from policies.AbstractPolicy import AbstractPolicy


class PhasedPolicy(AbstractPolicy):

    def __init__(self):
        pass

    def parse_query(self,query):
        query.parse(True)

    def new_query(self,query):
        return True
