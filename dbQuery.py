
import sqlparse
import time
import sys
import multiprocessing
import random

class dbQuery:
    
    
    def __init__(self, query_text, query_type_id, need_to_parse=True):
        self.query_id = random.randint(0,10000)
        self.id = self.query_id
        self.query_text = query_text
        self.read_identifiers = []
        self.write_identifiers = []
        self.query_type_id = query_type_id
        self.created_at = time.time()
        self.waiting_time = None
        self.completed = False
        if need_to_parse:
            self.parse()
        self.sql_obj=None

    def complete(self):
        self.total_time = time.time()-self.created_at
        self.completed = True
    
    def done_waiting(self):
        self.waiting_time = time.time() - self.created_at

    def parse(self):
        self.sql_obj = sqlparse.parse(self.query_text)[0] # Assumes only one query at a time for now
        self.generate_locks()
    
    def generate_locks(self):
        if self.sql_obj.get_type() == 'SELECT':
            for token in self.sql_obj.tokens:
                if type(token) is sqlparse.sql.Token and token.value=='SELECT':
                    in_identifier_list = True
                    in_table_list = False
                if type(token) is sqlparse.sql.Token and token.value=='FROM':
                    in_identifier_list = False
                    in_table_list = True
                
                if in_identifier_list and type(token) is sqlparse.sql.IdentifierList:
                    for identifier in token:
                        if type(identifier) is sqlparse.sql.Identifier:
                            self.add_identifier(identifier, self.read_identifiers)
                if in_identifier_list and type(token) is sqlparse.sql.Identifier:
                    self.add_identifier(token, self.read_identifiers)
                
                if type(token) is sqlparse.sql.Where:
                    for predicate in token:
                        if type(predicate) is sqlparse.sql.Comparison:
                            self.add_comparison(predicate,self.read_identifiers)
                        if type(predicate) is sqlparse.sql.Parenthesis:
                            for n_predicate in predicate.tokens:
                                if type(n_predicate) is sqlparse.sql.Comparison:
                                    self.add_comparison(n_predicate,self.read_identifiers)
                                
        if self.sql_obj.get_type() == 'UPDATE':
            for token in self.sql_obj.tokens:
                if type(token) is sqlparse.sql.Token and token.value=='UPDATE':
                    in_table_list = True
                    in_set_list = False
                if type(token) is sqlparse.sql.Token and token.value=='SET':
                    in_set_list = True
                    in_table_list = False
                    
                if in_set_list and type(token) is sqlparse.sql.IdentifierList:
                    for comparison in token:
                        if type(comparison) is sqlparse.sql.Comparison:
                            self.add_identifier(comparison[0],self.write_identifiers)
                            self.add_comparison(comparison, self.write_identifiers)
                if in_set_list and type(token) is sqlparse.sql.Comparison:
                    self.add_identifier(token,self.write_identifiers)
                    self.add_comparison(token, self.write_identifiers)
                        
                if type(token) is sqlparse.sql.Where:
                    for predicate in token:
                        if type(predicate) is sqlparse.sql.Comparison:
                            self.add_comparison(predicate,self.write_identifiers)
                        if type(predicate) is sqlparse.sql.Parenthesis:
                            for n_predicate in predicate.tokens:
                               if type(n_predicate) is sqlparse.sql.Comparison:
                                 self.add_comparison(n_predicate,self.read_identifiers)

        if self.sql_obj.get_type() == "INSERT":
            for token in self.sql_obj.tokens:
                if type(token) is sqlparse.sql.Function:
                    for parens in token.tokens:
                        if type(parens) is sqlparse.sql.Parenthesis:
                            for list in parens.tokens:
                                if type(list) is sqlparse.sql.IdentifierList:
                                    for id in list.tokens:
                                        if type(id) is sqlparse.sql.Identifier:
                                            self.add_identifier(id, self.write_identifiers)
                                
        if self.sql_obj.get_type() == "DELETE":
            for id in ["call_forwarding.s_id", "call_forwarding.sf_type", "call_forwarding.start_time", "call_forwarding.end_time", "call_forwarding.numberx"]:
                self.add_identifier(id, self.write_identifiers)
    

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

    def add_comparison(self, comparison, list):
      lock = {
        "column":str(comparison[0]),
        "lock_type":self.comparison_types(str(comparison[2])),
        "comparison_column":str(comparison[4]).replace("'",""),
        }
      
      # Comparing between columns means use ANY for both columns
      if '.' in lock['comparison_column']:
        self.add_identifier(lock['column'], list)
        self.add_identifier(lock['comparison_column'], list)
      else:
        list.append(lock)

    def add_identifier(self, identifier, list):
        list.append({"column":str(identifier),
                     "lock_type":0,
                     })

    def conflicts(self, other_query):
        for lock in self.write_identifiers:
          for other_lock in other_query.write_identifiers + other_query.read_identifiers:
            if dbQuery.do_locks_conflict(lock,other_lock):
              return True
        for lock in self.read_identifiers:
          for other_lock in other_query.write_identifiers:
            if dbQuery.do_locks_conflict(lock, other_lock):
              return True
        return False
    
    @staticmethod
    def do_locks_conflict(lock, other_lock):
        if lock['column'] == other_lock['column']:
            if lock['lock_type']=='ANY' or other_lock['lock_type']=='ANY':
                return True
        if dbQuery.compare_int_locks(lock, other_lock):
            return True
        return False
    
    @staticmethod
    def compare_int_locks(lock, other_lock):
        if (lock['lock_type']==1) and (other_lock['lock_type']==1) and (lock['comparison_column'] == other_lock['comparison_column']):
            return True
        # <= and =
        if (lock['lock_type']==5) and (other_lock['lock_type']==1) and (int(lock['comparison_column']) >= int(other_lock['comparison_column'])):
            return True
        # = and <=
        if (lock['lock_type']==1) and (other_lock['lock_type']==5) and (int(lock['comparison_column']) <= int(other_lock['comparison_column'])):
            return True
        # >= and =
        if (lock['lock_type']==4) and (other_lock['lock_type']==1) and (int(lock['comparison_column']) <= int(other_lock['comparison_column'])):
            return True
        # = and >=
        if (lock['lock_type']==1) and (other_lock['lock_type']==4) and (int(lock['comparison_column']) >= int(other_lock['comparison_column'])):
            return True
        # lock(>=) conflicts if <= lock(<=)
        if (lock['lock_type']==4) and (other_lock['lock_type']==5) and (int(lock['comparison_column']) <= int(other_lock['comparison_column'])):
            return True
        # <= and >=
        if (lock['lock_type']==5) and (other_lock['lock_type']==4) and (int(lock['comparison_column']) >= int(other_lock['comparison_column'])):
            return True
        # >= and >=
        if (lock['lock_type']==4) and (other_lock['lock_type']==5):
            return True
        # <= and <=
        if (other_lock['lock_type']==4) and (lock['lock_type']==1):
                return True
        return False

    def print_locks(self):
      print("Query: {}\n".format(self.query_text))
      for lock in self.write_identifiers:
        print(lock)
      #print("Write: {} {} {}\n".format(lock.column, lock.lock_type, lock.comparison_column))
      for lock in self.read_identifiers:
        print(lock)
#print("Read: {} {} {}\n".format(lock.column, lock.lock_type, lock.comparison_column))

