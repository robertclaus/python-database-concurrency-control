
class CompressedValue:
    def __init__(self, tabledotcolumn, type, value, mode):
        self.tabledotcolumn = tabledotcolumn
        self.type = type
        self.value = value
        self.mode = mode


class PredicateValue:
    ALL = 0
    EQ = 1
    READ = 1
    WRITE = 2

    def __init__(self, tabledotcolumn, type, value, mode):
        self.tabledotcolumn = tabledotcolumn
        self.type = type
        self.value = value
        self.column = tabledotcolumn.split('.')[1]
        self.table = tabledotcolumn.split('.')[0]
        self.mode = mode

    def compress(self):
        return CompressedValue(self.tabledotcolumn, self.type, self.value, self.mode)

    @staticmethod
    def decompress(cvalue):
        return PredicateValue(cvalue.tabledotcolumn, cvalue.type, cvalue.value, cvalue.mode)

    def __eq__(self, other):
        return (self.tabledotcolumn == other.tabledotcolumn and
                self.type == other.type and
                self.value == other.value and
                self.mode == other.mode)

    def __str__(self):
        return "Table:{}, Column:{}, Type:{}, Value:{}".format(self.table, self.column, self.type, self.value)

    def do_values_conflict(self, other_value):
        if self.mode == PredicateValue.WRITE or (self.mode == PredicateValue.READ and other_value.mode == PredicateValue.WRITE):
            # These checks occur in PredicateLock now
            #if self.tabledotcolumn == other_value.tabledotcolumn:
                # if self.table not in columns_to_consider or self.column in columns_to_consider[self.table]:
            if self.type == 0 or other_value.type == 0:
                return True
            if self.compare_int_locks(other_value):
                return True
        return False

    def compare_int_locks(self, other_value):
        lock1_type = self.type
        lock1_comparison = self.value
        lock2_type = other_value.type
        lock2_comparison = other_value.value
        if (lock1_type == 1) and (lock2_type == 1) and (lock1_comparison == lock2_comparison):
            return True
        # <= and =
        if (lock1_type == 5) and (lock2_type == 1) and (lock1_comparison >= lock2_comparison):
            return True
        # = and <=
        if (lock1_type == 1) and (lock2_type == 5) and (lock1_comparison <= lock2_comparison):
            return True
        # >= and =
        if (lock1_type == 4) and (lock2_type == 1) and (lock1_comparison <= lock2_comparison):
            return True
        # = and >=
        if (lock1_type == 1) and (lock2_type == 4) and (lock1_comparison >= lock2_comparison):
            return True
        # lock(>=) conflicts if <= lock(<=)
        if (lock1_type == 4) and (lock2_type == 5) and (lock1_comparison <= lock2_comparison):
            return True
        # <= and >=
        if (lock1_type == 5) and (lock2_type == 4) and (lock1_comparison >= lock2_comparison):
            return True
        # >= and >=
        if (lock1_type == 4) and (lock2_type == 4):
            return True
        # <= and <=
        if (lock1_type == 5) and (lock2_type == 5):
            return True
        return False
