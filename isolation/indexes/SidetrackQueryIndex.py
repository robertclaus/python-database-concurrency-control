class SidetrackQueryIndex:
    def __init__(self):
        self.sidetracked_readonly_queries = []
        self.sidetracked_write_queries = []
        self.sidetrack_indexes = {'columns_locked': {},
                                  'columns_locked_not_all': {},
                                  'columns_locked_write': {},
                                  'column_admit_rate': {},
                                  'mode': {},
                                  }

    def __len__(self):
        return len(self.sidetracked_write_queries) + len(self.sidetracked_readonly_queries)

    def add_queries(self, queries):
        if all([query.readonly for query in queries]):
            self.sidetracked_readonly_queries += queries
        else:
            for query in queries:
                self.add_query(query)

    def add_query(self, query):
        if query.readonly:
            self.sidetracked_readonly_queries.append(query)
        else:
            self.sidetracked_write_queries.append(query)

            for key, value in query.lock_indexes.items():  # Loop over indexes in query
                for column in query.lock_indexes[key]:  # Loop over columns in query
                    if column in self.sidetrack_indexes[key]:
                        self.sidetrack_indexes[key][column].append(query)  # Add query to index
                    else:
                        self.sidetrack_indexes[key][column] = [query]

    def remove_admitted_queries(self):
        self.sidetracked_readonly_queries = [q for q in self.sidetracked_readonly_queries if not q.was_admitted]
        self.sidetracked_write_queries = [q for q in self.sidetracked_write_queries if not q.was_admitted]

    def remove_queries(self, queries):
        if all([query.readonly for query in queries]):
            self.sidetracked_readonly_queries = [q for q in self.sidetracked_readonly_queries if not q in queries]
        else:
            for query in queries:
                self.remove_query(query)

    def remove_query(self, query):
        # print("Query: {}".format(query))
        if query.readonly:
            self.sidetracked_readonly_queries.remove(query)
        else:
            self.sidetracked_write_queries.remove(query)

            for index_name, value in query.lock_indexes.items():  # Loop over indexes in query
                for column in query.lock_indexes[index_name]:  # Loop over columns in query
                    if column in self.sidetrack_indexes[index_name] and query in self.sidetrack_indexes[index_name][
                        column]:
                        self.sidetrack_indexes[index_name][column].remove(query)

    def get_next_query(self):
        return self.sidetracked_queries[0]

    def take_read_only_queries(self):
        all_read_only_queries = self.sidetracked_readonly_queries
        self.sidetracked_readonly_queries = []
        return all_read_only_queries
