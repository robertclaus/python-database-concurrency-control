import Queue
import random
import time

class dbConcurrencyEngine:

#These are not used since we don't use this class as a static class anymore.
    waiting_queries = Queue.Queue()
    completed_queries = Queue.Queue()
    
    _waiting_query_list =[]
    
    _sidetracked_query_list = []
    _sidetrack_columns = {}
    
    _total_completed_queries = 0
    
    _archive_completed_queries = []
    
    def __init__(self, incoming_query_queues, used_a_query_cv, run_concurrency_check):
        # List of Queue's containing incoming queries
        self.incoming_query_queues = incoming_query_queues
        # A Queue of admitted queries
        self.waiting_queries = Queue.Queue()
        # A Queue of completed queries (Queries are moved from waiting to completed by client threads)
        self.complete_queries = Queue.Queue()
        # A list of admitted queries used to test locks against.  Queries are removed from this list by the process_completed_queries method
        self._waiting_query_list=[]
        # A list of admitted queries that can't run due to lock conflicts.
        self._sidetracked_query_list=[]
        self._total_completed_queries=0
        # The list of completed queries that have been removed from the _waiting_query_list already.
        self._archive_completed_queries=[]
        # A condition variable to signal incoming connections to place more on the queue.
        self.used_a_query_cv=used_a_query_cv
        
        self._sidetrack_columns = {}
    
        self.query_count=0
        self.default_conflict_function = lambda query1, query2: query1.conflicts(query2)
        self.conflict_function = self.default_conflict_function
        self.mode = 1
    
        self.run_concurrency_check = run_concurrency_check
        self.last_scheduler_change = time.time()
    
# Return the number of queries that have been admitted but not completed
    def queries_left(self):
        return self.waiting_queries.qsize() + len(self._sidetracked_query_list)

# Try to admit a list of new queries
    def append(self, new_queries, do_sidetrack=True):
      if self.mode == 1:
        for new_query in new_queries:
            self.proccess_completed_queries()
            conflicts = 0
            if self.run_concurrency_check:
                for existing_query in self._waiting_query_list:
                    if self.conflict_function(new_query, existing_query):
                        conflicts = conflicts+1
                        if do_sidetrack:
                          self.add_to_sidetrack(new_query)
                        break  # Break from the existing_query loop
            if conflicts == 0:
                self.waiting_queries.put(new_query)
                self._waiting_query_list.append(new_query)
                self.query_count += 1
                #print("Submitted Query {}".format(new_query.query_id))
                with self.used_a_query_cv:
                    self.used_a_query_cv.notify()
      return True

    def add_to_sidetrack(self, query):
      self._sidetracked_query_list.append(query)
      for column in query.columns_locked:
        if column in self._sidetrack_columns:
          self._sidetrack_columns[column].append(query)
        else:
          self._sidetrack_columns[column] = [query]

# Admit the next X random queries from the incoming query queues
    def append_next(self, queries_to_generate_at_a_time):
        new_queries = []
        for i in range(queries_to_generate_at_a_time):
            queue_to_use = random.randint(0,len(self.incoming_query_queues)-1)
            if self.incoming_query_queues[queue_to_use].qsize() > 0:
                new_queries.append(self.incoming_query_queues[queue_to_use].get())
            else:
                i=i-1
        self.append(new_queries)
          

# Remove completed queries from the _waiting_queries_list so their locks no longer get checked against
    def proccess_completed_queries(self):
      while not self.completed_queries.empty():
        try:
          complete_query = self.completed_queries.get()
          self._waiting_query_list.remove(complete_query)
          self._total_completed_queries=self._total_completed_queries+1
          self._archive_completed_queries.append(complete_query)
          for column in complete_query.columns_locked:
            if column in self._sidetrack_columns and complete_query in self._sidetrack_columns[column]:
              self._sidetrack_columns[column].remove(complete_query)
        except Queue.Empty:
          pass


# Try sidetracked queries again
    def move_sidetracked_queries(self, count):
        count = min(count, len(self._sidetracked_query_list))
        for i in range(count):
            query_to_retry =self._sidetracked_query_list.pop(0)
            if not query_to_retry.admitted_at is None:
              for column in query_to_retry.columns_locked:
                self._sidetrack_columns[column].remove(query_to_retry)
              self.append([query_to_retry])

# Return the total number of completed queries so far
    def total_completed_queries(self):
        return self._total_completed_queries





# Wait for all admitted (non-conflicting) queries to finish and process them
    def wind_down(self):
      while self.waiting_queries.qsize()>0:
        self.proccess_completed_queries()
      self.proccess_completed_queries()

# May change the accept mode to winding down or change the conflict function to be column specific.
    def consider_changing_lock_mode(self):
      #print("Time since last schedule change: {}",time.time()-self.last_scheduler_change)
      if len(self._sidetracked_query_list) > 0.5 * self.queries_left():
        if self.run_concurrency_check == True:
          for key, value in self._sidetrack_columns.items():
            print("Considering column: {}  with {} sidetracked entries.".format(key, len(value)))
            if len(value) > 30:
              print("Admitting {} queries of type {} isolated by column {}.".format(len(value), value[0].query_type_id, key))
              # Wind down current lock mode
              self.mode = 0
              self.wind_down()
        
              # Change conflict function to be column-specific
              self.conflict_function = lambda query1,query2: False

              self.mode = 1
              # Admit all queries in this set
              self.append(value[0:30],do_sidetrack=False)
              
              # Wind down current lock mode
              self.mode=0
              self.wind_down()
              self.mode=1

          # After looping over these return to standard lock mode by default
          self.conflict_function = self.default_conflict_function
