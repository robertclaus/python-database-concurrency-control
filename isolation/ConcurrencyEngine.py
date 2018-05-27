import Queue

import multiprocessing

import time

from isolation.indexes.GlobalLockIndex import GlobalLockIndex
from isolation.indexes.SidetrackQueryIndex import SidetrackQueryIndex


class dbConcurrencyEngine:

    def __init__(self, incoming_query_queues, used_a_query_cv, run_concurrency_check, query_completed_condition):
        manager = multiprocessing.Manager()

        # List of Queue's containing incoming queries
        self.incoming_query_queues = incoming_query_queues
        # A Queue of admitted queries
        self.waiting_queries = manager.Queue()
        # A Queue of completed queries (Queries are moved from waiting to completed by client threads)
        self.completed_queries = manager.Queue()
        # A list of admitted queries used to test locks against.  Queries are removed from this list by the process_completed_queries method
        self.lock_index = GlobalLockIndex()

        # A list of admitted queries that can't run due to lock conflicts.
        self.sidetrack_index = SidetrackQueryIndex()
        self._total_completed_queries = 0
        # The list of completed queries that have been removed from the global lock index already.
        self._archive_completed_queries = []
        # A condition variable to signal incoming connections to place more on the queue.
        self.used_a_query_cv = used_a_query_cv
        self.query_processed_cv = query_completed_condition

        self.query_count = 0
        self.cycle_count = 0

        self.run_concurrency_check = run_concurrency_check
        self.last_scheduler_change = time.time()

    # Return the number of queries that have been admitted but not completed
    def queries_left(self):
        return self.waiting_queries.qsize() + len(self.sidetrack_index)

    # Try to admit a list of new queries
    def admit_multiple(self, new_queries, place_on_sidetrack=True, remove_from_sidetrack=False, readonly=False):
        admitted = []
        not_admitted = []

        for new_query in new_queries:
            new_query.start_admit()
            if self.run_concurrency_check and not readonly and self.lock_index.does_conflict(new_query):
                not_admitted.append(new_query)
            else:
                admitted.append(new_query)
                if self.run_concurrency_check and not readonly:
                    self.lock_index.add_query(new_query)
                new_query.finish_admit()
                self.waiting_queries.put(new_query)

        if place_on_sidetrack:
            self.sidetrack_index.add_queries(not_admitted)

        if remove_from_sidetrack:
            self.sidetrack_index.remove_queries(admitted)

        self.query_count+=len(admitted)

        return admitted

    # Admit the next X random queries from the incoming query queues
    def append_next(self, queries_to_generate_at_a_time, straight_to_sidetrack=False):
        queries_admitted = 0
        main_queue = self.incoming_query_queues[0]
        if straight_to_sidetrack:
            while queries_admitted < queries_to_generate_at_a_time:
                try:
                    query = main_queue.get(False)
                    self.sidetrack_index.add_query(query)
                    queries_admitted += 1
                except Queue.Empty:
                    print(" ### Not generating queries fast enough.")
        else:
            while queries_admitted < queries_to_generate_at_a_time:
                try:
                    query = main_queue.get(False)
                    self.admit_multiple([query])
                    queries_admitted += 1
                except Queue.Empty:
                    print(" ### Not generating queries fast enough.")
        for i in xrange(10):
            with self.used_a_query_cv:
                self.used_a_query_cv.notify()

    # Remove completed queries from the _waiting_queries_list so their locks no longer get checked against
    def proccess_completed_queries(self):
        try:
            while True:
                complete_query = self.completed_queries.get_nowait()
                self._total_completed_queries = self._total_completed_queries + 1
                self._archive_completed_queries.append(complete_query)
                if self.run_concurrency_check and not complete_query.readonly:
                    self.lock_index.remove_query(complete_query)
        except Queue.Empty:
            pass

    # Return the total number of completed queries so far
    def total_completed_queries(self):
        return self._total_completed_queries

    # Wait for all admitted (non-conflicting) queries to finish and process them
    def wind_down(self):
        print("  Winding down. {}  Total:{}, Completed:{}".format(time.time(), self.query_count,
                                                                  self._total_completed_queries))
        while self.query_count != self._total_completed_queries:
            with self.query_processed_cv:
                self.query_processed_cv.wait(.01)
            self.proccess_completed_queries()
        self.proccess_completed_queries()
        print("  Wound down. {}  Total:{}, Completed:{}".format(time.time(), self.query_count,
                                                                self._total_completed_queries))

    # May change the accept mode to winding down or change the conflict function to be column specific.
    def consider_changing_lock_mode(self, max_sidetracked_queries, minimum_queue_size, target_queue_size):
        self.proccess_completed_queries()

        if self.run_concurrency_check == True:
            if len(self.sidetrack_index) > max_sidetracked_queries:

                # Admit all readonly queries
                self.wind_down()

                print("Readonly Start Admitting. {} ".format(time.time()))

                queries = self.sidetrack_index.take_read_only_queries()
                admitted_queries = self.admit_multiple(queries, place_on_sidetrack=True, remove_from_sidetrack=False,
                                               readonly=True)

                print("Readonly Finish Admitting {} queries. {} ".format(len(admitted_queries), time.time()))

                lock_combinations = [
                    ['call_forwarding.start_time','subscriber.sub_nbr'], # Delete
                    ['subscriber.sub_nbr'],# High Volume Update
                    ['special_facility.s_id','subscriber.s_id'],# Low Volume Update
                ]
                self.cycle_count += 1
                for combination in lock_combinations:
                    column_reference = {}
                    value = None
                    for column in combination:
                        column_queries = self.sidetrack_index.sidetrack_indexes['columns_locked_not_all'][column]
                        tab = column.split('.')[0]
                        col = column.split('.')[1]
                        if value is None:
                            value = column_queries
                        else:
                            value = [q for q in value if q in column_queries]

                        if not tab in column_reference:
                            column_reference[tab] = []
                        column_reference[tab].append(col)

                    if len(value) > minimum_queue_size:
                        start_depth = len(value)

                        # Wind down current lock mode
                        self.wind_down()

                        # Change conflict function to be column-specific
                        self.lock_index.set_scheduled_columns(column_reference)

                        admitted = 0
                        # Admit queries from this set
                        print("  Start Admitting. {} ".format(time.time()))
                        query_count = len(value)
                        queries = list(value)  # Make a copy so we can remove from it properly
                        admit_loops = 0
                        while len(queries) > target_queue_size:
                            self.proccess_completed_queries()
                            admit_loops += 1
                            queries_admitted = self.admit_multiple(queries,
                                                           place_on_sidetrack=False,
                                                           remove_from_sidetrack=True)
                            admitted += len(queries_admitted)
                            queries = [q for q in queries if q not in queries_admitted]

                        print("  Finish Admitting. Loops: {}  End Time: {} ".format(admit_loops, time.time()))
                        self.wind_down()

                        # Evaluate the performance of that column
                        end_depth = len(value)
                        print("Admitted {} of {} queries isolated by columns {}.".format(admitted, query_count,
                                                                                         ",".join(combination)))

                # After looping over these return to standard lock mode by default
        self.lock_index.set_scheduled_columns({})
