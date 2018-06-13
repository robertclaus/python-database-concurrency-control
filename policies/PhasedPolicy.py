from collections import defaultdict
import time

from isolation.indexes.GlobalLockIndex import GlobalLockIndex
from isolation.indexes.SidetrackQueryIndex import SidetrackQueryIndex
from policies.AbstractPolicy import AbstractPolicy
from queries.PredicateLock import NotSchedulableException


class PhasedPolicy(AbstractPolicy):

    def __init__(self):
        self.lock_index = GlobalLockIndex()
        self.sidetrack_index = SidetrackQueryIndex()
        self.queries_this_phase = []
        self.admitted_query_count = 0

        self.lock_combinations = [
                    ['call_forwarding.s_id', 'subscriber.sub_nbr', 'special_facility.s_id'],  # Insert
                    ['special_facility.s_id', 'subscriber.s_id'],  # Low Volume Update
                    ['call_forwarding.start_time','subscriber.sub_nbr'], # Delete
                    ['subscriber.sub_nbr'], # High Volume Update
                ]
        self.lock_combination_index = -2 # -1 is readonly, so the first phase will be -1.
        self.new_queries = []

    def parse_query(self,query):
        query.parse(True)

    def new_query(self, query):
        if self.lock_combination_index == -1 and query.readonly:
            self.admitted_query_count += 1
            return [query]

        self.new_queries.append(query)

        if self.admitted_query_count == 0:
            self.consider_changing_lock_mode()
            return self.admit_from_phase()

        return []

    def complete_query(self, query):
        self.admitted_query_count -= 1
        if not (self.lock_combination_index == -1 and query.readonly):
            self.lock_index.remove_query(query)

        if self.admitted_query_count == 0:
            self.consider_changing_lock_mode()

        return self.admit_from_phase()



    def admit_from_phase(self):
        if self.lock_combination_index == -1:
            self.admitted_query_count += len(self.queries_this_phase)
            read_only_queries = list(self.queries_this_phase)
            self.queries_this_phase = []
            return read_only_queries

        queries_to_return = []
        for query in self.queries_this_phase:
            if self.try_admit_query(query):
                queries_to_return.append(query)
                self.admitted_query_count += 1
                self.queries_this_phase.remove(query)
                if self.lock_combination_index != -1:
                    self.sidetrack_index.remove_query(query)
        return queries_to_return

    def try_admit_query(self, query):
        admit_as_readonly = self.lock_index.readonly and query.readonly
        sidetrack_if_not_readonly = self.lock_index.readonly
        try:
            if (not admit_as_readonly) and (sidetrack_if_not_readonly or self.lock_index.does_conflict(query)):
                return False
            else:
                self.lock_index.add_query(query)
                return True
        except NotSchedulableException:
            self.queries_this_phase.remove(query)
            print("Scheduling conflict")
            return False



    def consider_changing_lock_mode(self):
        while not self.queries_this_phase:
            print("Changing Phases {}".format(time.time()))
            self.lock_combination_index += 1
            if self.lock_combination_index == len(self.lock_combinations):
                self.lock_combination_index = -1

            if self.lock_combination_index == -1:
                self.start_read_only_phase()
            else:
                combination = self.lock_combinations[self.lock_combination_index]

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
                    self.start_column_phase(combination, column_reference, query_list)



    def start_read_only_phase(self):
        print("Readonly Start Admitting. {} ".format(time.time()))
        self.lock_index.read_only_mode(True)
        self.lock_index.set_scheduled_columns({})

        self.sidetrack_index.add_queries(self.new_queries)
        self.new_queries = []
        self.queries_this_phase = self.sidetrack_index.take_read_only_queries()


    def start_column_phase(self, combination, column_reference, query_list):
        print("Start Admitting {} queries at {} on columns: {}".format(len(query_list), time.time(),
                                                                       ",".join(combination)))
        self.lock_index.read_only_mode(False)
        self.lock_index.set_scheduled_columns(column_reference)
        self.queries_this_phase = list(query_list)