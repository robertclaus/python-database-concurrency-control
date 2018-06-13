import Queue

import multiprocessing

import time
from collections import defaultdict

import config
from isolation.indexes.GlobalLockIndex import GlobalLockIndex
from isolation.indexes.SidetrackQueryIndex import SidetrackQueryIndex

from policies.PhasedIntegratedPolicy import PhasedIntegratedPolicy

class IsolationManager:

    def __init__(self, dibs_policy, query_completed_condition, connector):
        manager = multiprocessing.Manager()

        self.connector=connector

        # A Queue of admitted queries
        self.waiting_queries = manager.Queue()
        # A Queue of completed queries (Queries are moved from waiting to completed by client threads)
        self.completed_queries = manager.Queue()

        self.query_count = 0
        self.completed_count = 0

        self.dibs_policy = dibs_policy

        IsolationManager.time_processing_completed = 0

        self.send_bundle_size = config.CLIENT_BUNDLE_SIZE

    # Return the number of queries that have been admitted but not completed
    def queries_left(self):
        return self.waiting_queries.qsize()*self.send_bundle_size

    # Try to admit a list of new queries
    def admit_multiple(self, new_queries):
        query_bundle = []
        admitted = []

        for new_query in new_queries:
            new_query.start_admit()

            queries_to_admit = self.dibs_policy.new_query(new_query)

            for query in queries_to_admit:
                admitted.append(query)
                query.finish_admit()
                query_bundle.append(query.copy_light())
                if len(query_bundle) > self.send_bundle_size:
                    self.waiting_queries.put(query_bundle)
                    query_bundle = []

        if query_bundle:
            self.waiting_queries.put(query_bundle)

        self.query_count += len(admitted)

        return admitted

    # Admit the next X random queries from the incoming query queues
    def append_next(self, queries_to_generate_at_a_time):
        queries_admitted = 0

        while queries_admitted < queries_to_generate_at_a_time:
            queries = self.connector.next_queries()
            self.admit_multiple(queries)
            queries_admitted += len(queries)
            self.proccess_completed_queries()

    # Remove completed queries from the _waiting_queries_list so their locks no longer get checked against
    def proccess_completed_queries(self):
        try:
            start = time.time()
            query_bundle = []
            while True:
                try:
                    complete_query = self.completed_queries.get_nowait()
                    self.completed_count += 1

                    self.connector.complete_query(complete_query)
                    queries_to_admit = self.dibs_policy.complete_query(complete_query)

                    for query in queries_to_admit:
                        query.finish_admit()
                        query_bundle.append(query.copy_light())
                        if len(query_bundle) > self.send_bundle_size:
                            self.waiting_queries.put(query_bundle)
                            query_bundle = []
                except Queue.Empty:
                    break

            if query_bundle:
                self.waiting_queries.put(query_bundle)

            IsolationManager.time_processing_completed += (time.time() - start)
        except IOError:
            print("#### IO ERROR - Likely Broken Pipe")

    # Return the total number of completed queries so far.  This can be in the archive or the completed queue itself.
    def total_completed_queries(self):
        return self.completed_queries.qsize() + self.completed_count