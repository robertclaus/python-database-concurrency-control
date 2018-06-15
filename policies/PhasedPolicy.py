from collections import defaultdict
import time

import config
from isolation.indexes.GlobalLockIndex import GlobalLockIndex
from isolation.indexes.SidetrackQueryIndex import SidetrackQueryIndex
from policies.AbstractPolicy import AbstractPolicy
from queries.PredicateLock import NotSchedulableException


class PhasedPolicy(AbstractPolicy):

    def __init__(self):
        self.lock_index = GlobalLockIndex()
        self.sidetrack_index = SidetrackQueryIndex()

        self.queries_this_phase = []
        self.lock_combination_index = -2 # -1 is readonly, so the first phase will be -1.

        self.queries_next_phase = []
        self.next_lock_combination_index = -1
        self.column_reference_next_phase = {}
        self.new_queries_for_next_phase = []
        self.prep_next_phase = True

        self.admitted_query_count = 0

        self.lock_combinations = [
                    ['call_forwarding.s_id', 'subscriber.sub_nbr', 'special_facility.s_id'],  # Insert
                    ['special_facility.s_id', 'subscriber.s_id'],  # Low Volume Update
                    ['call_forwarding.start_time','subscriber.sub_nbr'], # Delete
                    ['subscriber.sub_nbr'], # High Volume Update
                ]

        self.new_queries = []
        self.aborted = False

    def parse_query(self,query):
        query.parse(True)

    def new_query(self, query):
        if self.prep_next_phase:
            self.select_next_lock_mode()

        if self.lock_combination_index == -1 and query.readonly:
            self.admitted_query_count += 1
            return [query]

        self.new_queries.append(query)

        if self.admitted_query_count == 0:
            self.transition_lock_mode()
            return self.admit_from_phase()

        return []

    def complete_query(self, query):
        if self.prep_next_phase:
            self.select_next_lock_mode()

        self.admitted_query_count -= 1
        if not (self.lock_combination_index == -1 and query.readonly):
            self.lock_index.remove_query(query)

        if self.admitted_query_count == 0:
            self.transition_lock_mode()
            return self.admit_from_phase()

        if self.admitted_query_count < (len(self.queries_this_phase)*2):
            return self.admit_from_phase()

        if self.queries_this_phase and len(self.queries_this_phase) < self.min_queries_this_phase():
            self.delay_remaining_queries()

        return []

    def delay_remaining_queries(self):
        print("Delay phase with {} queries left.".format(len(self.queries_this_phase)))
        self.new_queries.extend(self.queries_this_phase)
        self.queries_this_phase = []

    def admit_from_phase(self):
        if self.lock_combination_index == -1:
            self.admitted_query_count += len(self.queries_this_phase)
            read_only_queries = list(self.queries_this_phase)
            self.queries_this_phase = []
            return read_only_queries

        queries_to_return = []
        queries_to_remove = []
        queries_added = 0

        for query in self.queries_this_phase:
            query.start_admit_time = time.time() # Override admit time on the query
            if self.can_admit_query(query):
                queries_to_return.append(query)
                self.admitted_query_count += 1
                queries_to_remove.append(query)
                self.lock_index.add_query(query)
                queries_added+=1
                if queries_added > config.QUERIES_TO_ADMIT_AT_TIME:
                    break

        self.queries_this_phase = [query for query in self.queries_this_phase if query not in queries_to_remove]

        print("Admitted {} queries, with {} remaining. {}".format(self.admitted_query_count, len(self.queries_this_phase), time.time()))
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
            self.queries_this_phase.remove(query)
            print("Scheduling conflict")
            return False

    def min_queries_this_phase(self):
        if self.lock_combination_index == -1:
            return config.MIN_QUERIES_TO_ADMIT_READONLY
        return config.MIN_QUERIES_TO_ADMIT

    def transition_lock_mode(self):
        self.prep_next_phase = True
        self.lock_combination_index = self.next_lock_combination_index
        self.queries_this_phase = self.queries_next_phase

        if self.lock_combination_index == -1:
            print("Admitting Reads {}".format(time.time()))
            self.lock_index.read_only_mode(True)
            self.lock_index.set_scheduled_columns({})
        else:
            combination = self.lock_combinations[self.lock_combination_index]
            column_reference = self.column_reference_next_phase
            query_list = self.queries_this_phase
            print("Tried Admitting {} queries at {} on columns: {}".format(len(query_list), time.time(),
                                                                           ",".join(combination)))
            self.lock_index.read_only_mode(False)
            self.lock_index.set_scheduled_columns(column_reference)
            self.sidetrack_index.remove_queries(self.queries_this_phase)

    def select_next_lock_mode(self):
        self.prep_next_phase = False
        abort_count = 0
        while not self.queries_next_phase:
            abort_count += 1
            if abort_count > len(self.lock_combinations)+1:
                break

            print("Changing Phases {}".format(time.time()))
            self.next_lock_combination_index += 1
            if self.next_lock_combination_index == len(self.lock_combinations):
                self.next_lock_combination_index = -1

            if self.next_lock_combination_index == -1:
                self.new_queries_for_next_phase = self.new_queries[:config.MAX_QUERIES_PER_PHASE]
                self.new_queries = self.new_queries[config.MAX_QUERIES_PER_PHASE:]
                self.sidetrack_index.add_queries(self.new_queries_for_next_phase)
                self.queries_next_phase = self.sidetrack_index.take_read_only_queries()
            else:
                combination = self.lock_combinations[self.next_lock_combination_index]

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

                if query_list is not None:# and len(value) > minimum_queue_size:
                    self.queries_next_phase = list(query_list)
                    self.column_reference_next_phase = column_reference
