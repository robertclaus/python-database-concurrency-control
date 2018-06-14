from Queue import Empty
import multiprocessing
import time
import config

class IsolationManager:

    def __init__(self, dibs_policy, connector):
        self.connector = connector
        self.dibs_policy = dibs_policy
        self.send_bundle_size = config.CLIENT_BUNDLE_SIZE

        manager = multiprocessing.Manager()
        # A Queue of admitted queries
        self.waiting_queries = manager.Queue()
        self.active_queries = {}
        # A Queue of completed queries (Queries are moved from waiting to completed by client threads)
        self.completed_queries = manager.Queue()

        self.query_count = 0
        self.completed_count = 0

        IsolationManager.time_processing_completed = 0

    # Return the number of queries that have been admitted but not completed
    def queries_left(self):
        return self.waiting_queries.qsize()*self.send_bundle_size

    # Return the total number of completed queries so far.  This can be in the archive or the completed queue itself.
    def total_completed_queries(self):
        return self.completed_queries.qsize() + self.completed_count

    # Try to admit a list of new queries to the database clients
    def admit_multiple(self, new_queries):
        query_bundle = []

        for new_query in new_queries:
            new_query.start_admit_time = time.time()
            queries_to_admit = self.dibs_policy.new_query(new_query)
            self.add_queries_to_bundle(queries_to_admit, query_bundle)

        if query_bundle:
            self.waiting_queries.put(query_bundle)

        return

    # Admit the next queries from the connector
    def append_next(self, queries_to_generate_at_a_time):
        previous_query_count= self.query_count

        while (self.query_count - previous_query_count) < queries_to_generate_at_a_time:
            queries = self.connector.next_queries()
            self.admit_multiple(queries)
            self.proccess_completed_queries()

    # Process any queries completed by the database clients so the connector can complete them
    def proccess_completed_queries(self):
        try:
            start = time.time()
            query_bundle = []
            while True:
                try:
                    complete_micro_query = self.completed_queries.get_nowait()
                    self.completed_count += 1
                    complete_query = self.active_queries.pop(complete_micro_query.query_id)
                    complete_query.merge_micro(complete_micro_query)

                    self.connector.complete_query(complete_query)
                    queries_to_admit = self.dibs_policy.complete_query(complete_query)

                    self.add_queries_to_bundle(queries_to_admit, query_bundle)
                except Empty:
                    break

            if query_bundle:
                self.waiting_queries.put(query_bundle)

            IsolationManager.time_processing_completed += (time.time() - start)
        except IOError:
            print("#### IO ERROR - Likely Broken Pipe Between Processes")


    def add_queries_to_bundle(self, queries, query_bundle):
        for query in queries:
            query.finish_admit_time = time.time()
            query.time_to_admit = query.finish_admit_time - query.start_admit_time
            query.was_admitted = True

            self.active_queries[query.query_id] = query
            query_bundle.append(query.copy_micro())
            self.query_count+=1

            if len(query_bundle) > self.send_bundle_size:
                self.waiting_queries.put(query_bundle)
                query_bundle.clear()