

class LockValue:
    ALL = 0
    EQ = 1
    
    def __init__(self, tabledotcolumn, type, value, mode):
        self.tabledotcolumn = tabledotcolumn
        self.type = type
        self.value = value
        self.column = tabledotcolumn.split('.')[1]
        self.table = tabledotcolumn.split('.')[0]
        self.mode = mode
        
    def __eq__(self, other):
        return (self.tabledotcolumn == other.tabledotcolumn and
                self.type == other.type and
                self.value == other.value and
                self.mode == other.mode)
        
    def __str__(self):
        return "Table:{}, Column:{}, Type:{}, Value:{}".format(self.table, self.column, self.type, self.value)

    def do_values_conflict(self, other_value, columns_to_consider):
        if self.mode==PredicateLock.WRITE or (self.mode==PredicateLock.READ and other_value.mode==PredicateLock.WRITE):
            if self.tabledotcolumn == other_value.tabledotcolumn:
                if self.table not in columns_to_consider or self.column in columns_to_consider[self.table]:
                    if self.type==0 or other_value.type==0:
                        return True
                    if self.compare_int_locks(other_value):
                        return True
        return False

    def compare_int_locks(self, other_value):
        lock1_type = self.type
        lock1_comparison = self.value
        lock2_type = other_value.type
        lock2_comparison = other_value.value
        if (lock1_type==1) and (lock2_type==1) and (lock1_comparison == lock2_comparison):
            return True
        # <= and =
        if (lock1_type==5) and (lock2_type==1) and (lock1_comparison >= lock2_comparison):
            return True
        # = and <=
        if (lock1_type==1) and (lock2_type==5) and (lock1_comparison <= lock2_comparison):
            return True
        # >= and =
        if (lock1_type==4) and (lock2_type==1) and (lock1_comparison <= lock2_comparison):
            return True
        # = and >=
        if (lock1_type==1) and (lock2_type==4) and (lock1_comparison >= lock2_comparison):
            return True
        # lock(>=) conflicts if <= lock(<=)
        if (lock1_type==4) and (lock2_type==5) and (lock1_comparison <= lock2_comparison):
            return True
        # <= and >=
        if (lock1_type==5) and (lock2_type==4) and (lock1_comparison >= lock2_comparison):
            return True
        # >= and >=
        if (lock1_type==4) and (lock2_type==4):
            return True
        # <= and <=
        if (lock1_type==5) and (lock2_type==5):
            return True
        return False

class PredicateLock:
    WRITE = 2
    READ = 1
    
    def __init__(self):
        self.lockvalues = []
        self.tabledotcolumnindex = {}
        self.tableindex=[]
        self.tableandcolumnindex={}
        self.notalltabledotcolumnindex = {}
        self.nonequality_value_count = 0
        self.equality_index = {}
        self.readonly=True

    def add_value(self, tabledotcolumn, type, value, mode):
        lockvalue = LockValue(tabledotcolumn,type,value, mode)
        self.lockvalues.append(lockvalue)
        
        if not tabledotcolumn in self.tabledotcolumnindex:
            self.tabledotcolumnindex[tabledotcolumn] = []
        self.tabledotcolumnindex[tabledotcolumn].append(lockvalue)
        
        if not lockvalue.table in self.equality_index:
            self.equality_index[lockvalue.table] = {}
        if not lockvalue.column in self.equality_index[lockvalue.table]:
            self.equality_index[lockvalue.table][lockvalue.column] = {}
        if lockvalue.type==1:
            self.equality_index[lockvalue.table][lockvalue.column][lockvalue.value]=True
        
        if not lockvalue.table in self.tableandcolumnindex:
            self.tableandcolumnindex[lockvalue.table]={}
        if not lockvalue.column in self.tableandcolumnindex[lockvalue.table]:
            self.tableandcolumnindex[lockvalue.table][lockvalue.column]=[]
        self.tableandcolumnindex[lockvalue.table][lockvalue.column].append(lockvalue)
        
        if lockvalue.table not in self.tableindex:
            self.tableindex.append(lockvalue.table)
        
        if type != LockValue.ALL:
            if not tabledotcolumn in self.notalltabledotcolumnindex:
                self.notalltabledotcolumnindex[tabledotcolumn] = []
            self.notalltabledotcolumnindex[tabledotcolumn].append(lockvalue)

        if type != 1:
            self.nonequality_value_count+=1
        
        if mode != PredicateLock.READ:
            self.readonly=False

    def remove_value(self, value):
        self.lockvalues.remove(value)
        self.tabledotcolumnindex[value.tabledotcolumn].remove(value)
        self.tableandcolumnindex[value.table][value.column].remove(value)
        if value.type != LockValue.ALL:
            self.notalltabledotcolumnindex[value.tabledotcolumn].remove(value)
        if value.type!=1:
            self.nonequality_value_count-=1
        if value.type==1:
            self.equality_index[value.table][value.column].pop(value.value, None)

    def locked_values_for(self, table, column):
        if table in self.tableandcolumnindex and column in self.tableandcolumnindex[table]:
            return self.tableandcolumnindex[table][column]
        return []

    def locked_columns_for(self, table):
        if table in self.tableandcolumnindex:
            for col in self.tableandcolumnindex[table]:
                yield col

    def value_exists(self, tabledotcolumn, mode):
        if any(v for v in self.lockvalues if v.mode==mode and v.tabledotcolumn==tabledotcolumn):
            return True
        return False

    # Initially just get rid of ALL locks that we have more specific locks for.
    def merge_values(self):
        for value in self.lockvalues:
            for other_value in self.lockvalues:
                if value!=other_value and value in self.lockvalues and other_value in self.lockvalues:
                    if value.tabledotcolumn == other_value.tabledotcolumn and value.type==0 and value.mode==PredicateLock.READ and value.mode==PredicateLock.READ:
                        self.remove_value(value)
                    if value.tabledotcolumn == other_value.tabledotcolumn and value.type==0 and value.mode==PredicateLock.WRITE and value.mode==PredicateLock.WRITE:
                        self.remove_value(other_value)

    def do_locks_conflict(self, other_lock, columns_to_consider={}):
        for value in self.lockvalues:
            if value.table in other_lock.tableandcolumnindex and value.column in other_lock.tableandcolumnindex[value.table]:
                for other_value in other_lock.tableandcolumnindex[value.table][value.column]:
                    if value.do_values_conflict(other_value,columns_to_consider):
                        return True
        return False

    def __str__(self):
        return_string = ""
        return_string+="\nWrite Locks:\n"
        for value in [v for v in self.lockvalues if v.mode==PredicateLock.WRITE]:
            return_string+=str(value)+"\n"
        return_string+="\nRead Locks:\n"
        for value in [v for v in self.lockvalues if v.mode==PredicateLock.READ]:
            return_string+=str(value)+"\n"
        return return_string
