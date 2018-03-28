import random
import string
from dbQuery import dbQuery
import time
import Queue
import threading

class dbQueryGenerator:

    class replacePattern:
        def __init__ (self, pattern, lambda_function):
            self._lambda_function = lambda_function
            self._pattern = pattern

        def replace(self, target):
            return target.replace(self._pattern, self._lambda_function(target))
    
    wild_card_rules = [
        replacePattern("<randInt>", lambda s: str(random.randint(1,100000))),
        replacePattern("<randInt2>", lambda s: str(random.randint(1,100000))),
        replacePattern("<randInt3>", lambda s: str(random.randint(1,100000))),
        replacePattern("<query_obj_id>", lambda s: str(dbQuery.query_id)),
    ]

    generator_id = 0

    def __init__(self, possible_query_list, need_to_parse, target_depth,
                 num_worker_threads, run_in_series):
        self.threads = []
        self.total_thread_count = num_worker_threads
        self.possible_query_list = possible_query_list
        self.generated_query_queue = Queue.Queue()
        self.generator_id = dbQueryGenerator.generator_id
        dbQueryGenerator.generator_id = dbQueryGenerator.generator_id + 1
        
        for i in range(num_worker_threads):
            t = threading.Thread(target=dbQueryGenerator.worker,
                                 args=[self.generated_query_queue, possible_query_list, need_to_parse, target_depth, run_in_series, dbQueryGenerator.generator_id],)
            t.setDaemon(True)
            t.start()
            self.threads.append(t)

    def end_processes(self):
        # stop workers
        for i in range(self.total_thread_count):
            self.waiting_queue.put(None)
            
        self.waiting_queue.put(None)
        while self.waiting_queue.get() is not None:
            self.waiting_queue.task_done()
            pass

    @staticmethod
    def worker(waiting_queue, possible_query_list, need_to_parse, target_depth, run_in_series, generator_id):
        while True:
            queries_to_generate = target_depth - waiting_queue.qsize()
            while queries_to_generate > 0:
                index = random.randint(0,len(possible_query_list)-1)
                query_text = possible_query_list[index]
                for replace_rule in dbQueryGenerator.wild_card_rules:
                    query_text = replace_rule.replace(query_text)
                last_query = dbQuery(query_text, generator_id*1000 + index, need_to_parse)
                waiting_queue.put(last_query)
                while run_in_series and not last_query.completed:
                    time.sleep(.001)
                queries_to_generate=queries_to_generate-1
        time.sleep(.001)
