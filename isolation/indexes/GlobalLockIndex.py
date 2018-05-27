

class GlobalLockIndex:

    def __init__(self):
        self.locking_queries = []
        self.column_reference = {}
        self.equality_index = {}

    def set_scheduled_columns(self, column_reference):
        self.column_reference = column_reference

    def add_queries(self, queries):
        map(self.add_query,queries)

    def add_query(self,query):
        self.locking_queries.append(query)

    def remove_queries(self, queries):
        map(self.remove_query, queries)

    def remove_query(self, query):
        self.locking_queries.remove(query)

    def does_conflict(self, query):
        return any([query.conflicts(existing_query) for existing_query in self.locking_queries])