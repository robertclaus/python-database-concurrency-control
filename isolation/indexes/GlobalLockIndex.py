class GlobalLockIndex:

    def __init__(self):
        self.locking_queries = []
        self.column_reference = {}
        self.non_equality_values = 0
        self.equality_index = {}

    def set_scheduled_columns(self, column_reference):
        self.column_reference = column_reference

    def add_queries(self, queries):
        map(GlobalLockIndex.add_query,queries)

    def add_query(self,query):
        self.locking_queries.append(query)
        self.non_equality_values += query.predicatelock.nonequality_value_count

    def remove_queries(self, queries):
        map(GlobalLockIndex.remove_query, queries)

    def remove_queries(self, query):
        self.locking_queries.remove(query)
        self.non_equality_values -= query.predicatelock.nonequality_value_count

    def does_conflict(self, query):
        for existing_query in self.locking_queries:
            if query.conflicts(existing_query, self.column_reference):
                return True
        return False