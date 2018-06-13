import Queue

import multiprocessing

import time
from collections import defaultdict

from isolation.indexes.GlobalLockIndex import GlobalLockIndex
from isolation.indexes.SidetrackQueryIndex import SidetrackQueryIndex

from policies.PhasedIntegratedPolicy import PhasedIntegratedPolicy

class IsolationManager:

    def __init__(self, dibs_policy, query_completed_condition, send_bundle_size, connector):
        manager = multiprocessing.Manager()

        self.connector=connector

        # A Queue of admitted queries
        self.waiting_queries = manager.Queue()
        # A Queue of completed queries (Queries are moved from waiting to completed by client threads)
        self.completed_queries = manager.Queue()
        # A list of admitted queries used to test locks against.  Queries are removed from this list by the process_completed_queries method
        self.lock_index = GlobalLockIndex()

        # A list of admitted queries that can't run due to lock conflicts.
        self.sidetrack_index = SidetrackQueryIndex()
        # The list of completed queries that have been removed from the global lock index already.
        self._archive_completed_queries = connector.finished_list

        self.query_processed_cv = query_completed_condition

        self.query_count = 0
        IsolationManager.cycle_count = 0

        self.dibs_policy = dibs_policy
        self.last_scheduler_change = time.time()

        IsolationManager.time_processing_completed = 0

        self.send_bundle_size = send_bundle_size

        self.run_phased_policy = isinstance(self.dibs_policy, PhasedIntegratedPolicy)

    # Return the number of queries that have been admitted but not completed
    def queries_left(self):
        return self.waiting_queries.qsize()*self.send_bundle_size + len(self.sidetrack_index)

    # Try to admit a list of new queries
    def admit_multiple(self, new_queries, already_on_sidetrack=False, sidetrack_if_not_readonly=False):
        query_bundle = []
        admitted = []
        not_admitted = []

        for new_query in new_queries:
            new_query.start_admit()

            if self.run_phased_policy:

                admit_as_readonly = self.lock_index.readonly and new_query.readonly

                if self.run_phased_policy and (not admit_as_readonly) and (sidetrack_if_not_readonly or self.lock_index.does_conflict(new_query)):
                    not_admitted.append(new_query)
                else:
                    admitted.append(new_query)
                    if self.run_phased_policy and not admit_as_readonly:
                        self.lock_index.add_query(new_query)
                    new_query.finish_admit()
                    query_bundle.append(new_query.copy_light())
                    if len(query_bundle) > self.send_bundle_size:
                        self.waiting_queries.put(query_bundle)
                        query_bundle = []
            else:
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

        if self.run_phased_policy and not already_on_sidetrack:
            self.sidetrack_index.add_queries(not_admitted)

        if self.run_phased_policy and already_on_sidetrack:
            self.sidetrack_index.remove_admitted_queries()

        self.query_count += len(admitted)

        return admitted

    # Admit the next X random queries from the incoming query queues
    def append_next(self, queries_to_generate_at_a_time):


        queries_admitted = 0

        while queries_admitted < queries_to_generate_at_a_time:
            queries = self.connector.next_queries()
            self.admit_multiple(queries, already_on_sidetrack=False, sidetrack_if_not_readonly=True)
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
                    self._archive_completed_queries.append(complete_query)
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
        return self.completed_queries.qsize() + len(self._archive_completed_queries)

    # Wait for all admitted (non-conflicting) queries to finish and process them
    def wind_down(self):
        print("  Winding down. {}  Total:{}, Completed:{}".format(time.time(), self.query_count,
                                                                  self.total_completed_queries()))
        while self.query_count != self.total_completed_queries():
            with self.query_processed_cv:
                self.query_processed_cv.wait(.01)
        self.lock_index.clear_all_queries()
        print("  Wound down. {}  Total:{}, Completed:{}".format(time.time(), self.query_count,
                                                                self.total_completed_queries()))

    # May change the accept mode to winding down or change the conflict function to be column specific.
    def consider_changing_lock_mode(self, max_sidetracked_queries, minimum_queue_size, target_queue_size):

        if self.run_phased_policy == True:
            if len(self.sidetrack_index) > max_sidetracked_queries:

                # Admit all readonly queries - Happens automatically when admitting queries the first time, but may as well leave it
                print("Readonly Start Admitting. {} ".format(time.time()))

                self.lock_index.read_only_mode(True)
                queries = self.sidetrack_index.take_read_only_queries()
                admitted_queries = self.admit_multiple(queries, already_on_sidetrack=False)

                print("Readonly Finish Admitting {} queries. {} ".format(len(admitted_queries), time.time()))

                lock_combinations = [
                    ['call_forwarding.s_id', 'subscriber.sub_nbr', 'special_facility.s_id'],  # Insert
                    ['special_facility.s_id', 'subscriber.s_id'],  # Low Volume Update
                    ['call_forwarding.start_time','subscriber.sub_nbr'], # Delete
                    ['subscriber.sub_nbr'], # High Volume Update
                ]
                IsolationManager.cycle_count += 1
                for combination in lock_combinations:
                    column_reference = defaultdict(list)
                    value = None
                    for column in combination:
                        column_queries = self.sidetrack_index.sidetrack_indexes['columns_locked'][column]

                        tab = column.split('.')[0]
                        col = column.split('.')[1]
                        if value is None:
                            value = column_queries
                        else:
                            value = [q for q in value if q in column_queries]

                        column_reference[tab].append(col)

                    if value is not None:# and len(value) > minimum_queue_size:
                        start_depth = len(value)

                        self.lock_index.read_only_mode(False)
                        # Wind down current lock mode
                        self.wind_down()

                        # Change conflict function to be column-specific
                        self.lock_index.set_scheduled_columns(column_reference)

                        admitted = 0
                        # Admit queries from this set
                        print("Start Admitting. {}  Columns: {}".format(time.time(), ",".join(combination)))
                        query_count = len(value)
                        queries = list(value)  # Make a copy so we can remove from it properly
                        admit_loops = 0
                        while len(queries) > target_queue_size:
                            admit_loops += 1
                            queries_admitted = self.admit_multiple(queries, already_on_sidetrack=True)
                            admitted += len(queries_admitted)
                            if len(queries_admitted)<2:
                                print("Only admitted one query:\n{}\n".format(queries_admitted[0]))
                            for query in queries_admitted:
                                queries.remove(query)
                            self.wind_down()
                            print("We have {} queries left".format(len(queries)))

                        print("Finish Admitting {} of {} queries. Loops: {}  End Time: {} Columns: {}".format(admitted, query_count,
                                                                                                              admit_loops, time.time(),
                                                                                                              ",".join(combination)))

                # Always get new queries in readonly mode so they can be admitted immediately.
                if not self.lock_index.readonly:
                    self.wind_down()
                    self.lock_index.read_only_mode(True)

                # After looping over these return to standard lock mode by default
                self.lock_index.set_scheduled_columns({})
