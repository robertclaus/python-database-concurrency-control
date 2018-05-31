import sqlparse
import time
import sys
import multiprocessing
import random

from PredicateLock import PredicateLock


class dbQuery:
    READ = 1
    WRITE = 2

    def __init__(self, query_text, query_type_id):
        self.query_id = random.randint(0, 100000000)
        self.id = self.query_id
        self.query_text = query_text
        self.predicatelock = PredicateLock()
        self.query_type_id = query_type_id
        self.created_at = time.time()
        self.waiting_time = None
        self.admitted_at = None
        self.completed = False
        self.sql_obj = None
        self.worker = None
        self.worker_waited_time = None
        self.lock_run_under = None
        self.lock_indexes = {'columns_locked': [],
                             'columns_locked_not_all': [],
                             'columns_locked_write': [],
                             'tables_locked': [],
                             'mode': [],
                             }
        self.error = None
        self.start_admit_time = 0
        self.finish_admit_time = 0
        self.time_to_admit = 0
        self.readonly = True
        self.was_admitted = False

    def copy_light(self):
        small_query = dbQuery(self.query_text, self.query_type_id)
        small_query.query_id = self.query_id
        small_query.id = self.query_id
        small_query.query_text = self.query_text
        small_query.created_at = self.created_at
        small_query.waiting_time = self.waiting_time
        small_query.admitted_at = self.admitted_at
        small_query.start_admit_time = self.start_admit_time
        small_query.finish_admit_time = self.finish_admit_time
        small_query.time_to_admit = self.time_to_admit
        small_query.was_admitted = self.was_admitted
        return small_query

    def __eq__(self, other):
        return self.query_id == other.query_id

    def start_admit(self):
        self.start_admit_time = time.time()

    def finish_admit(self):
        self.finish_admit_time = time.time()
        self.time_to_admit = self.finish_admit_time - self.start_admit_time
        self.was_admitted = True

    def log_error(self, error):
        self.error = error

    def complete(self):
        self.total_time = time.time() - self.created_at
        self.completed = True

    def done_waiting(self):
        self.waiting_time = time.time() - self.created_at

    def admitted(self):
        self.admitted_at = time.time()

    def parse(self):
        self.sql_obj = sqlparse.parse(self.query_text)[0]  # Assumes only one query at a time for now
        self.generate_locks()
        self.strip_fluff()
        self.predicatelock.merge_values()
        self.generate_lock_indexes()

    def strip_fluff(self):
        self.sql_obj = None

    def compress(self):
        self.predicatelock = None
        self.lock_indexes = None

    def generate_lock_indexes(self):
        for tabledotcolumn in self.predicatelock.tabledotcolumnindex:
            self.lock_indexes['columns_locked'].append(tabledotcolumn)
        for tabledotcolumn in self.predicatelock.notalltabledotcolumnindex:
            self.lock_indexes['columns_locked_not_all'].append(tabledotcolumn)
        if self.predicatelock.readonly:
            self.readonly = True
        else:
            self.readonly = False

    def get_locked_columns(self):
        return self.lock_indexes['columns_locked']

    def get_locked_tables(self):
        return self.predicatelock.tableindex

    def generate_locks(self):
        if self.sql_obj.get_type() == 'SELECT':
            for token in self.sql_obj.tokens:
                if type(token) is sqlparse.sql.Where:
                    for predicate in token:
                        if type(predicate) is sqlparse.sql.Comparison:
                            self.add_comparison(predicate, PredicateLock.READ)
                        if type(predicate) is sqlparse.sql.Parenthesis:
                            for n_predicate in predicate.tokens:
                                if type(n_predicate) is sqlparse.sql.Comparison:
                                    self.add_comparison(n_predicate, PredicateLock.READ)

            for token in self.sql_obj.tokens:
                if type(token) is sqlparse.sql.Token and token.value == 'SELECT':
                    in_identifier_list = True
                    in_table_list = False
                if type(token) is sqlparse.sql.Token and token.value == 'FROM':
                    in_identifier_list = False
                    in_table_list = True

                if in_identifier_list and type(token) is sqlparse.sql.IdentifierList:
                    for identifier in token:
                        if type(identifier) is sqlparse.sql.Identifier:
                            if not self.predicatelock.value_exists(PredicateLock.READ, str(identifier)):
                                self.add_identifier(identifier, PredicateLock.READ)
                if in_identifier_list and type(token) is sqlparse.sql.Identifier:
                    if not self.predicatelock.value_exists(PredicateLock.READ, str(token)):
                        self.add_identifier(token, PredicateLock.READ)

        if self.sql_obj.get_type() == 'UPDATE':
            for token in self.sql_obj.tokens:
                if type(token) is sqlparse.sql.Where:
                    for predicate in token:
                        if type(predicate) is sqlparse.sql.Comparison:
                            self.add_comparison(predicate, PredicateLock.READ)
                        if type(predicate) is sqlparse.sql.Parenthesis:
                            for n_predicate in predicate.tokens:
                                if type(n_predicate) is sqlparse.sql.Comparison:
                                    self.add_comparison(n_predicate, PredicateLock.READ)

            for token in self.sql_obj.tokens:
                if type(token) is sqlparse.sql.Token and token.value == 'UPDATE':
                    in_table_list = True
                    in_set_list = False
                if type(token) is sqlparse.sql.Token and token.value == 'SET':
                    in_set_list = True
                    in_table_list = False

                if in_set_list and type(token) is sqlparse.sql.IdentifierList:
                    for comparison in token:
                        if type(comparison) is sqlparse.sql.Comparison:
                            if not self.predicatelock.value_exists(PredicateLock.READ, str(comparison[0])):
                                self.add_identifier(comparison[0], PredicateLock.WRITE)
                            self.add_comparison(comparison, PredicateLock.WRITE)

                if in_set_list and type(token) is sqlparse.sql.Comparison:
                    if not self.predicatelock.value_exists(PredicateLock.READ, str(token[0])):
                        self.add_identifier(token[0], PredicateLock.WRITE)
                    self.add_comparison(token, PredicateLock.WRITE)

        if self.sql_obj.get_type() == "INSERT":
            for token in self.sql_obj.tokens:
                if type(token) is sqlparse.sql.Function:
                    for parens in token.tokens:
                        if type(parens) is sqlparse.sql.Parenthesis:
                            for list in parens.tokens:
                                if type(list) is sqlparse.sql.IdentifierList:
                                    for id in list.tokens:
                                        if type(id) is sqlparse.sql.Identifier:
                                            self.add_identifier(id, PredicateLock.WRITE)
            for token in self.sql_obj.tokens:
                if type(token) is sqlparse.sql.Where:
                    for predicate in token:
                        if type(predicate) is sqlparse.sql.Comparison:
                            self.add_comparison(predicate, PredicateLock.READ)
                            if type(predicate) is sqlparse.sql.Parenthesis:
                                for n_predicate in predicate.tokens:
                                    if type(n_predicate) is sqlparse.sql.Comparison:
                                        self.add_comparison(n_predicate, PredicateLock.READ)

            for id in ["subscriber.s_id", "special_facility.s_id"]:  # ,"subscriber.sub_nbr"]:
                self.add_identifier(id, PredicateLock.WRITE)

        if self.sql_obj.get_type() == "DELETE":
            for token in self.sql_obj.tokens:
                if type(token) is sqlparse.sql.Comparison:
                    self.add_comparison(token, PredicateLock.WRITE)
                if type(token) is sqlparse.sql.Where:
                    for predicate in token:
                        if type(predicate) is sqlparse.sql.Comparison:
                            self.add_comparison(predicate, PredicateLock.WRITE)
            for id in ["call_forwarding.sf_type", "call_forwarding.numberx"]:
                self.add_identifier(id, PredicateLock.WRITE)

    def comparison_types(self, comparison_string):
        if comparison_string == '=':
            return 1
        if comparison_string == '>':
            return 2
        if comparison_string == '<':
            return 3
        if comparison_string == '>=':
            return 4
        if comparison_string == '<=':
            return 5
        print("Failed to recognize comparison type.")
        return -1

    def add_comparison(self, comparison, mode):
        main_column = str(comparison[0])
        comparison_column = str(comparison[4]).replace("'", "")

        # Comparing between columns means use ANY for both columns
        if '.' in comparison_column:
            self.add_identifier(main_column, mode)
            self.add_identifier(comparison_column, mode)
        else:
            self.predicatelock.add_value(main_column,
                                         self.comparison_types(str(comparison[2])),
                                         comparison_column,
                                         mode)

    def add_identifier(self, identifier, mode):
        self.predicatelock.add_value(str(identifier),
                                     0,
                                     "",
                                     mode)

    def conflicts(self, other_query, columns_to_consider={}):
        return self.predicatelock.do_locks_conflict(other_query.predicatelock, columns_to_consider)

    def __str__(self):
        return "Query: {}\n{}\nReadonly:{}".format(self.query_text, str(self.predicatelock),self.readonly)
