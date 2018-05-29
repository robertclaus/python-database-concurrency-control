

class GlobalLockIndex:

    def __init__(self):
        self.locking_queries = []
        self.column_reference = {}
        self.equality_index = {}
        self.readonly = False

    def set_scheduled_columns(self, column_reference):
        self.column_reference = column_reference

    def add_queries(self, queries):
        map(self.add_query,queries)

    def add_query(self,query):
        self.locking_queries.append(query)

    def clear_all_queries(self):
        self.locking_queries=[]

    def remove_queries(self, queries):
        map(self.remove_query, queries)

    def remove_query(self, query):
        self.locking_queries.remove(query)

    def read_only_mode(self, readonly):
        self.readonly = readonly

    def does_conflict(self, query):
        if self.readonly:
            return False

        for existing_query in self.locking_queries:
            if query.conflicts(existing_query, self.column_reference):
                return True
        return False
        return any([query.conflicts(existing_query) for existing_query in self.locking_queries])