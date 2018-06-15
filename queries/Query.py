import sqlparse
import time
import sys
import multiprocessing
import random

from PredicateLock import PredicateLock

class microQuery:
    def __init__(self, query_id, query_text, created_at):
        self.id = query_id
        self.query_id = query_id
        self.query_text = query_text
        self.created_at = created_at
        self.result = None
        self.error = None

        self.worker_waited_time = 0
        self.worker = -1
        self.completed_at = -1
        self.total_time = -1
        self.waiting_time = -1

class compressedQuery:
    def __init__(self, query_id, query_text, compressed_lock, query_type, created_at):
        self.query_id = query_id
        self.query_text = query_text
        self.predicatelock = compressed_lock
        self.query_type_id = query_type
        self.created_at = created_at


class dbQuery:
    READ = 1
    WRITE = 2

    def __init__(self, query_text, query_type_id):
        self.query_id = random.randint(0, 223372036854775807)
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
        self.tables_locked = []
        self.result = None
        self.lock_indexes = {'columns_locked': [],
                             'columns_locked_not_all': [],
                             'columns_locked_write': [],
                             'mode': [],
                             }
        self.error = None
        self.start_admit_time = 0
        self.finish_admit_time = 0
        self.time_to_admit = 0
        self.readonly = True
        self.was_admitted = False
        self.completed_at = None

    def compress(self):
        return compressedQuery(self.query_id, self.query_text, self.predicatelock.compress(), self.query_type_id, self.created_at)

    @staticmethod
    def decompress(compressed_query):
        dquery = dbQuery(compressed_query.query_text, compressed_query.query_type_id)
        dquery.query_id = compressed_query.query_id
        dquery.id = compressed_query.query_id
        dquery.predicatelock = PredicateLock.decompress(compressed_query.predicatelock)
        dquery.generate_lock_indexes()
        return dquery


    def copy_micro(self):
        return microQuery(self.query_id, self.query_text, self.created_at)

    def merge_micro(self, micro):
        self.result = micro.result
        self.error = micro.error

        self.worker_waited_time = micro.worker_waited_time
        self.worker = micro.worker
        self.completed_at = micro.completed_at
        self.total_time = micro.total_time
        self.waiting_time = micro.waiting_time

    def __eq__(self, other):
        return self.query_id == other.query_id

    def start_admit(self):
        self.start_admit_time = time.time()

    def time_since_admit(self):
        return time.time() - self.start_admit_time

    def finish_admit(self):
        self.finish_admit_time = time.time()
        self.time_to_admit = self.finish_admit_time - self.start_admit_time
        self.was_admitted = True

    def log_error(self, error):
        self.error = error

    def complete(self):
        self.completed_at = time.time()
        self.total_time = self.completed_at - self.created_at
        self.completed = True

    def done_waiting(self):
        self.waiting_time = time.time() - self.created_at

    def admitted(self):
        self.admitted_at = time.time()

    def parse(self,skip_read_only=False):
        self.sql_obj = sqlparse.parse(self.query_text)[0]  # Assumes only one query at a time for now
        self.generate_locks(skip_read_only)
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
        for table in self.predicatelock.tableindex:
            self.tables_locked.append(table)
        if self.predicatelock.readonly:
            self.readonly = True
        else:
            self.readonly = False

    def get_locked_columns(self):
        return self.lock_indexes['columns_locked']

    def get_locked_tables(self):
        return self.predicatelock.tableindex

    def generate_locks(self, skip_read_only):
        if self.sql_obj.get_type() == 'SELECT':
            self.readonly = True
            if skip_read_only:
                return
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
                            self.add_comparison(predicate, PredicateLock.WRITE)
                        if type(predicate) is sqlparse.sql.Parenthesis:
                            for n_predicate in predicate.tokens:
                                if type(n_predicate) is sqlparse.sql.Comparison:
                                    self.add_comparison(n_predicate, PredicateLock.WRITE)

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
                            if not self.predicatelock.value_exists(PredicateLock.WRITE, str(comparison[0])):
                                self.add_identifier(comparison[0], PredicateLock.WRITE)
                            self.add_comparison(comparison, PredicateLock.WRITE)

                if in_set_list and type(token) is sqlparse.sql.Comparison:
                    if not self.predicatelock.value_exists(PredicateLock.WRITE, str(token[0])):
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
                            self.add_comparison(predicate, PredicateLock.WRITE)
                            if type(predicate) is sqlparse.sql.Parenthesis:
                                for n_predicate in predicate.tokens:
                                    if type(n_predicate) is sqlparse.sql.Comparison:
                                        self.add_comparison(n_predicate, PredicateLock.WRITE)

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
