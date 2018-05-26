class GlobalLockIndex:

    def __init__(self):
        self.locking_queries = []
        self.column_reference = {}
        self.non_equality_values = 0
        self.equality_index = {}

    def set_scheduled_columns(self, column_reference):
        self.column_reference = column_reference

    def add_query(self, query):
        self.locking_queries.append(query)
        self.non_equality_values += query.predicatelock.nonequality_value_count
        """for table in query.predicatelock.equality_index:
            if not table in self.equality_index:
                self.equality_index[table]={}
            for column in query.predicatelock.equality_index[table]:
                if not column in self.equality_index[table]:
                    self.equality_index[table][column]={}
                for value in query.predicatelock.equality_index[table][column]:
                    if value in self.equality_index[table][column]:
                        self.equality_index[table][column][value]+=1
                    else:
                        self.equality_index[table][column][value]=1"""

    def remove_query(self, query):
        self.locking_queries.remove(query)
        self.non_equality_values -= query.predicatelock.nonequality_value_count
        """for table in query.predicatelock.equality_index:
            for column in query.predicatelock.equality_index[table]:
                for value in query.predicatelock.equality_index[table][column]:
                    self.equality_index[table][column][value]-=1"""

    def does_conflict(self, query):
        can_use_index = False  # (self.non_equality_values == 0)

        if can_use_index:
            return self.check_conflict_with_index(query)
        else:
            return self.check_conflict_fully(query)

    def check_conflict_fully(self, query):
        for existing_query in self.locking_queries:
            if query.conflicts(existing_query, self.column_reference):
                # print("----------\nQueries conflicted:\n\n{}\n\n{}".format(query, existing_query))
                return True
        return False

    def check_conflict_with_index(self, query):
        # print("Index yay!!")
        for table in query.get_locked_tables():
            if table in self.column_reference:
                for column in self.column_reference(table):
                    for value in query.predicatelock.locked_values_for(table, column):
                        if value.type == 1:  # Equality
                            if value.value in self.equality_index[table][column]:
                                return True
                            else:
                                pass
                        else:  # Ranges default to normal behavior
                            return self.check_conflict_fully(query)
            else:
                for column in query.predicatelock.locked_columns_for(table):
                    for value in query.predicatelock.locked_values_for(table, column):
                        if value.type == 1:  # Equality
                            if table in self.equality_index and column in self.equality_index[table] and value.value in \
                                    self.equality_index[table][column]:
                                return True
                            else:
                                pass
                        else:  # Ranges default to normal behavior
                            return self.check_conflict_fully(query)
        return False
