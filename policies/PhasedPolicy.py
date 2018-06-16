from collections import defaultdict
import time

import config
from isolation.indexes.GlobalLockIndex import GlobalLockIndex
from isolation.indexes.SidetrackQueryIndex import SidetrackQueryIndex
from policies.AbstractPolicy import AbstractPolicy
from queries.PredicateLock import NotSchedulableException


# Prep one phase at a time to make it faster instead of all at once


class Phase():
    def __init__(self, queries, readonly, column_references):
        self.queries = queries
        self.readonly = readonly
        self.column_reference = column_references
        self.lock_index = GlobalLockIndex()

        if self.readonly:
            self.lock_index.read_only_mode(True)
            self.lock_index.set_scheduled_columns({})
        else:
            self.lock_index.read_only_mode(False)
            self.lock_index.set_scheduled_columns(self.column_reference)

        self.initial_set = self.admit_from_phase(True)

    def min_queries_this_phase(self):
        if self.readonly:
            return config.MIN_QUERIES_TO_ADMIT_READONLY
        return config.MIN_QUERIES_TO_ADMIT

    def admit_from_phase(self, initial_admit):
        if self.readonly:
            queries = list(self.queries)
            self.queries = []
            return queries

        queries_to_return = []
        queries_to_remove = []
        queries_added = 0

        for query in self.queries:
            query.start_admit_time = time.time() # Override admit time on the query
            if self.can_admit_query(query):
                queries_to_return.append(query)
                queries_to_remove.append(query)
                self.lock_index.add_query(query)
                queries_added+=1
                if initial_admit and queries_added > config.QUERIES_TO_INITIALLY_ADMIT:
                    break
                if queries_added > config.QUERIES_TO_ADMIT_AT_TIME:
                    break

        self.queries = [query for query in self.queries if query not in queries_to_remove]
        return queries_to_return

    def can_admit_query(self, query):
        admit_as_readonly = self.lock_index.readonly and query.readonly
        sidetrack_if_not_readonly = self.lock_index.readonly
        try:
            if (not admit_as_readonly) and (sidetrack_if_not_readonly or self.lock_index.does_conflict(query)):
                return False
            else:
                return True
        except NotSchedulableException:
            self.queries.remove(query)
            print("Scheduling conflict")
            return False

    def complete_query(self, query):
        if not (self.readonly and query.readonly):
            self.lock_index.remove_query(query)

class PhasedPolicy(AbstractPolicy):

    def __init__(self):
        self.sidetrack_index = SidetrackQueryIndex()
        self.admitted_query_count = 0

        self.lock_combinations = [
                    ['call_forwarding.s_id', 'subscriber.sub_nbr', 'special_facility.s_id'],  # Insert
                    ['special_facility.s_id', 'subscriber.s_id'],  # Low Volume Update
                    ['call_forwarding.start_time','subscriber.sub_nbr'], # Delete
                    ['subscriber.sub_nbr'], # High Volume Update
                ]

        self.phases = []
        self.new_queries = []
        self.current_phase = Phase([],True,{})

    def parse_query(self,query):
        query.parse(True)

    def new_query(self, query):
        if self.current_phase.readonly and query.readonly:
            self.admitted_query_count += 1
            return [query]

        self.new_queries.append(query)

        if len(self.phases) == 0:
            self.prep_new_phases()

        if self.admitted_query_count == 0:
            self.start_next_phase()
            queries = self.current_phase.initial_set
            self.admitted_query_count += len(queries)
            print(
                "Admitted {} queries. Remaining: {}  {}".format(len(queries), len(self.current_phase.queries), time.time()))
            return queries

        return []

    def complete_query(self, query):
        self.admitted_query_count -= 1
        self.current_phase.complete_query(query)

        if len(self.phases) == 0:
            self.prep_new_phases()

        if self.admitted_query_count == 0:
            self.start_next_phase()
            queries = self.current_phase.initial_set
            self.admitted_query_count += len(queries)
            print(
                "Admitted {} queries. Remaining: {}  {}".format(len(queries), len(self.current_phase.queries), time.time()))
            return queries

        if self.admitted_query_count < (len(self.current_phase.queries)*2):
            queries = self.current_phase.admit_from_phase(False)
            self.admitted_query_count += len(queries)
            print(
                "Admitted {} queries. Remaining: {}  {}".format(len(queries), len(self.current_phase.queries), time.time()))
            return queries

        if self.current_phase.queries and len(self.current_phase.queries) < self.current_phase.min_queries_this_phase():
            self.delay_remaining_queries()

        return []

    def delay_remaining_queries(self):
        print("Delay phase with {} queries left.".format(len(self.current_phase.queries)))
        self.new_queries.extend(self.current_phase.queries)
        self.current_phase.queries = []

    def prep_new_phases(self):
        print("Start Prep Phases {}".format(time.time()))
        self.add_new_queries()
        self.add_readonly_phase()
        for combination in self.lock_combinations:
            self.add_phase(combination)
        self.phases.reverse()
        print("End Prep Phases {}".format(time.time()))

    def start_next_phase(self):
        while (self.current_phase.queries == 0) and self.phases:
            self.current_phase = self.phases.pop()
        if len(self.current_phase.queries) == 0:
            self.current_phase = Phase([], True, {})
        phase = self.current_phase
        print("Starting Phase  Time {}  Count: {}  Readonly: {}  Columns: {}".format(time.time(), len(phase.queries),
                                                                                     phase.readonly,
                                                                                     phase.column_reference))

    def add_new_queries(self):
        slice_of_new_queries = self.new_queries[:config.MAX_QUERIES_PER_PHASE]
        self.new_queries = self.new_queries[config.MAX_QUERIES_PER_PHASE:]
        self.sidetrack_index.add_queries(slice_of_new_queries)

    def add_readonly_phase(self):
        queries = self.sidetrack_index.take_read_only_queries()
        self.phases.append(Phase(queries, True, {}))

    def add_phase(self, combination):
        column_reference = defaultdict(list)
        query_list = None
        for column in combination:
            column_queries = self.sidetrack_index.sidetrack_indexes['columns_locked'][column]

            tab = column.split('.')[0]
            col = column.split('.')[1]
            if query_list is None:
                query_list = column_queries
            else:
                query_list = [q for q in query_list if q in column_queries]

            column_reference[tab].append(col)

        if query_list is not None:  # and len(value) > minimum_queue_size:
            queries = list(query_list)
            self.phases.append(Phase(queries, False, column_reference))
            self.sidetrack_index.remove_queries(queries)