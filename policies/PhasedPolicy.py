from collections import defaultdict
import time

from isolation.indexes.GlobalLockIndex import GlobalLockIndex
from isolation.indexes.SidetrackQueryIndex import SidetrackQueryIndex
from policies.AbstractPolicy import AbstractPolicy


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
        self.lock_combination_index = -1 # -1 is readonly

    def parse_query(self,query):
        query.parse(True)

    def new_query(self, query):
        if self.lock_combination_index == -1 and query.readonly:
            self.admitted_query_count += 1
            return [query]

        self.lock_index.add_query(query)

        if self.admitted_query_count == 0:
            if len(self.sidetrack_index) > 1000:
                self.consider_changing_lock_mode()

            return self.admit_from_phase(already_on_sidetrack=True)

        return []

    def complete_query(self, query):
        self.admitted_query_count -= 1
        if not (self.lock_combination_index == -1 and query.readonly):
            self.lock_index.remove_query(query)

        if self.admitted_query_count == 0:
            if len(self.sidetrack_index) > 1000:
                self.consider_changing_lock_mode()

        return self.admit_from_phase(already_on_sidetrack=True)




    def admit_from_phase(self, already_on_sidetrack):
        queries_to_return = []
        for query in self.queries_this_phase:
            if self.try_admit_query(query, already_on_sidetrack):
                queries_to_return.append(query)
                self.admitted_query_count += 1
                self.queries_this_phase.remove(query)
        return queries_to_return

    def start_read_only_phase(self):
        # Admit all readonly queries - Happens automatically when admitting queries the first time, but may as well leave it
        print("Readonly Start Admitting. {} ".format(time.time()))
        self.lock_index.read_only_mode(True)
        self.queries_this_phase = self.sidetrack_index.take_read_only_queries()


    def start_column_phase(self, combination, column_reference, query_list):
        print("Start Admitting {} queries at {} on columns: {}".format(len(query_list), time.time(),
                                                                       ",".join(combination)))
        self.lock_index.read_only_mode(False)
        # Change conflict function to be column-specific
        self.lock_index.set_scheduled_columns(column_reference)
        # Admit queries from this set
        self.queries_this_phase = list(query_list)

    def try_admit_query(self, query, already_on_sidetrack):
        admit_as_readonly = self.lock_index.readonly and query.readonly
        sidetrack_if_not_readonly = self.lock_index.readonly

        if (not admit_as_readonly) and (sidetrack_if_not_readonly or self.lock_index.does_conflict(query)):
            if not already_on_sidetrack:
                self.sidetrack_index.add_queries([query])
            return []
        else:
            if not admit_as_readonly:
                self.lock_index.add_query(query)
            if already_on_sidetrack:
                self.sidetrack_index.remove_query(query)
            return [query]

    def consider_changing_lock_mode(self):
        self.lock_combination_index = self.lock_combination_index + 1
        if self.lock_combination_index == len(self.lock_combinations):
            self.lock_combination_index = -1

        if self.lock_combination_index == -1:
            self.lock_index.read_only_mode(True)
            self.lock_index.set_scheduled_columns({})
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
