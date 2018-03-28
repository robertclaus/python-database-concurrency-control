
import sqlparse
import time
import sys

class dbQuery:
    
    query_id = 1;
    
    def __init__(self, query_text, query_type_id, need_to_parse=True):
        self.id = dbQuery.query_id
        dbQuery.query_id = dbQuery.query_id + 1
        self.query_text = query_text
        self.read_identifiers = []
        self.write_identifiers = []
        self.query_type_id = query_type_id
        self.created_at = time.time()
        self.waiting_time = None
        self.completed = False
        if need_to_parse:
            self.parse()

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
                    for comparison in token:
                        if type(comparison) is sqlparse.sql.Comparison:
                            self.add_comparison(comparison,self.read_identifiers)
                                
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
                    self.add_identifier(token[0],self.write_identifiers)
                    self.add_comparison(token, self.write_identifiers)
                        
                if type(token) is sqlparse.sql.Where:
                    for comparison in token:
                        if type(comparison) is sqlparse.sql.Comparison:
                            self.add_comparison(comparison,self.write_identifiers)


    def add_comparison(self, comparison, list):
        list.append({
                     "column":str(comparison[0]),
                     "lock_type":str(comparison[1]),
                     "comparison_column":str(comparison[2]),
                     })

    def add_identifier(self, identifier, list):
        list.append({"column":str(identifier),
                     "lock_type":'ANY',
                     })

    def do_columns_conflict(self, other_query):
        for lock in self.write_identifiers:
            for other_lock in other_query.write_identifiers + other_query.read_identifiers:
                if dbQuery.do_locks_conflict(lock,other_lock):
                    #print("Found conflict between: <"+str(self.query_text)+" - "+str(lock['column'])+"> and <"+str(other_query.query_text)+">")
                    return True
        for lock in self.read_identifiers:
            for other_lock in other_query.write_identifiers:
                if dbQuery.do_locks_conflict(lock, other_lock):
                    #print("Found conflict between: <"+str(self.query_text)+" - "+str(lock['column'])+"> and <"+str(other_query.query_text)+">")
                    return True
        #print("Found no conflict between: <"+str(self.query_text)+"> and <"+str(other_query.query_text)+">")
        return False

    def conflicts(self, other_query):
        do_conflict = self.do_columns_conflict(other_query)
        return do_conflict
    
    @staticmethod
    def do_locks_conflict(lock, other_lock):
        if lock['column'] == other_lock['column']:
            if lock['lock_type']=='ANY' or other_lock['lock_type']=='ANY':
                return True
            if (lock['lock_type']=='=') and (other_lock['lock_type']=='=') and (lock['comparison_column'] == other_lock['comparison_column']):
                return True
        return False
