from PredicateValue import PredicateValue

from collections import defaultdict

def default_dict_function():
    return defaultdict(list)

class PredicateLock:
    WRITE = 2
    READ = 1

    def __init__(self):
        self.predicatevalues = []
        self.tabledotcolumnindex = {}
        self.tableindex = []
        self.tableandcolumnindex = defaultdict(default_dict_function)
        self.notalltabledotcolumnindex = {}
        self.nonequality_value_count = 0
        self.equality_index = {}
        self.readonly = True

    def add_value(self, tabledotcolumn, type, value, mode):
        predicatevalue = PredicateValue(tabledotcolumn, type, value, mode)
        self.predicatevalues.append(predicatevalue)

        if not tabledotcolumn in self.tabledotcolumnindex:
            self.tabledotcolumnindex[tabledotcolumn] = []
        self.tabledotcolumnindex[tabledotcolumn].append(predicatevalue)

        if not predicatevalue.table in self.equality_index:
            self.equality_index[predicatevalue.table] = {}
        if not predicatevalue.column in self.equality_index[predicatevalue.table]:
            self.equality_index[predicatevalue.table][predicatevalue.column] = {}
        if predicatevalue.type == 1:
            self.equality_index[predicatevalue.table][predicatevalue.column][predicatevalue.value] = True

        self.tableandcolumnindex[predicatevalue.table][predicatevalue.column].append(predicatevalue)

        if predicatevalue.table not in self.tableindex:
            self.tableindex.append(predicatevalue.table)

        if type != PredicateValue.ALL:
            if not tabledotcolumn in self.notalltabledotcolumnindex:
                self.notalltabledotcolumnindex[tabledotcolumn] = []
            self.notalltabledotcolumnindex[tabledotcolumn].append(predicatevalue)

        if type != 1:
            self.nonequality_value_count += 1

        if mode != PredicateLock.READ:
            self.readonly = False

    def remove_value(self, value):
        self.predicatevalues.remove(value)
        self.tabledotcolumnindex[value.tabledotcolumn].remove(value)
        self.tableandcolumnindex[value.table][value.column].remove(value)
        if value.type != PredicateValue.ALL:
            self.notalltabledotcolumnindex[value.tabledotcolumn].remove(value)
        if value.type != 1:
            self.nonequality_value_count -= 1
        if value.type == 1:
            self.equality_index[value.table][value.column].pop(value.value, None)

    def locked_values_for(self, table, column):
        return self.tableandcolumnindex[table][column]

    def locked_columns_for(self, table):
        for col in self.tableandcolumnindex[table]:
            yield col

    def value_exists(self, tabledotcolumn, mode):
        if any(v for v in self.predicatevalues if v.mode == mode and v.tabledotcolumn == tabledotcolumn):
            return True
        return False

    # Initially just get rid of ALL locks that we have more specific locks for.
    def merge_values(self):
        for value in self.predicatevalues:
            for other_value in self.predicatevalues:
                if value != other_value and value in self.predicatevalues and other_value in self.predicatevalues:
                    if value.tabledotcolumn == other_value.tabledotcolumn and value.type == 0 and value.mode == PredicateLock.READ and value.mode == PredicateLock.READ:
                        self.remove_value(value)
                    if value.tabledotcolumn == other_value.tabledotcolumn and value.type == 0 and value.mode == PredicateLock.WRITE and value.mode == PredicateLock.WRITE:
                        self.remove_value(other_value)

    def do_locks_conflict(self, other_lock, columns_to_consider={}):
        columns_that_conflict = defaultdict(list)

        for table_accessed in self.tableandcolumnindex:
            if table_accessed not in columns_to_consider:
                print("Attempted to schedule a query while one of it's tables wasn't touched by scheduled locks.\nQuery:\n{}\nLocks:\n{}".format(self, columns_to_consider))
                return True

        for table in columns_to_consider:
            for column in columns_to_consider[table]:
                if not self.tableandcolumnindex[table][column]:
                    columns_that_conflict[table].append(column)
                    print("No reference to {}.{} in query 1 means a conflict on it.".format(table,column))
                    # There is a conflict on table and column because this column is ANY

                for value in self.tableandcolumnindex[table][column]:
                    if not other_lock.tableandcolumnindex[table][column]: # No reference in other lock -> ANY -> conflict
                        # There is a conflict on table and column
                        columns_that_conflict[table].append(column)
                        print("No reference to {}.{} in query 2 means a conflict on it.".format(table, column))
                    for other_value in other_lock.tableandcolumnindex[table][column]:
                        # Test for conflict
                        if value.do_values_conflict(other_value):
                            print("Conflict on values:\n{}\n{}\n".format(value, other_value))
                            columns_that_conflict[table].append(column)

        for table in columns_to_consider:
            for column in columns_to_consider[table]:
                if column not in columns_that_conflict[table]:
                    return False

        print("\nConflict:\nQuery 1:\n{}\nQuery 2:\n{}\n\n".format(self, other_lock))
        return True

        for value in self.predicatevalues:
            if value.table in other_lock.tableandcolumnindex and value.column in other_lock.tableandcolumnindex[value.table]:
                for other_value in other_lock.tableandcolumnindex[value.table][value.column]:
                    if value.do_values_conflict(other_value, columns_to_consider):
                        return True
        return False

    def __str__(self):
        return_string = ""
        return_string += "\nWrite Locks:\n"
        for value in [v for v in self.predicatevalues if v.mode == PredicateLock.WRITE]:
            return_string += str(value) + "\n"
        return_string += "\nRead Locks:\n"
        for value in [v for v in self.predicatevalues if v.mode == PredicateLock.READ]:
            return_string += str(value) + "\n"
        return return_string
