import Queue
import random


class dbConcurrencyEngine:

    waiting_queries = Queue.Queue()
    completed_queries = Queue.Queue()
    
    _waiting_query_list =[]
    _sidetracked_query_list = []
    
    _total_completed_queries = 0
    
    _archive_completed_queries = []
    
    def __init__(self, incoming_query_queues, used_a_query_cv):
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
    
        self.query_count=0
    
# Return the number of queries that have been admitted but not completed
    def queries_left(self):
        return self.waiting_queries.qsize() + len(self._sidetracked_query_list)

# Try to admit a list of new queries
    def append(self, new_queries, run_concurrency_check):
        for new_query in new_queries:
            self.proccess_completed_queries()
            conflicts = 0
            if run_concurrency_check:
                for existing_query in self._waiting_query_list:
                    if new_query.conflicts(existing_query):
                        conflicts = conflicts+1
                        self._sidetracked_query_list.append(new_query)
                        #print("Conflict between <{}> and <{}>".format(existing_query.query_text, new_query.query_text))
                        break  # Break from the existing_query loop
            if conflicts == 0:
                self.waiting_queries.put(new_query)
                self._waiting_query_list.append(new_query)
                self.query_count += 1
                #print("Submitted Query {}".format(new_query.query_id))
                with self.used_a_query_cv:
                    self.used_a_query_cv.notify()
        return True

# Admit the next X random queries from the incoming query queues
    def append_next(self, queries_to_generate_at_a_time, run_concurrency_check):
        new_queries = []
        for i in range(queries_to_generate_at_a_time):
            queue_to_use = random.randint(0,len(self.incoming_query_queues)-1)
            if self.incoming_query_queues[queue_to_use].qsize() > 0:
                new_queries.append(self.incoming_query_queues[queue_to_use].get())
            else:
                i=i-1
        self.append(new_queries, run_concurrency_check)
          

# Remove completed queries from the _waiting_queries_list so their locks no longer get checked against
    def proccess_completed_queries(self):
        while not self.completed_queries.empty():
            complete_query = self.completed_queries.get()
            self._waiting_query_list.remove(complete_query)
            self._total_completed_queries=self._total_completed_queries+1
            self._archive_completed_queries.append(complete_query)

# Try sidetracked queries again
    def move_sidetracked_queries(self, count):
        count = min(count, len(self._sidetracked_query_list))
        for i in range(count):
            query_to_retry =self._sidetracked_query_list.pop(0)
            #print("Sidetrack retry {}".format(query_to_retry.query_id))
            self.append([query_to_retry],True)

# Return the total number of completed queries so far
    def total_completed_queries(self):
        return self._total_completed_queries
