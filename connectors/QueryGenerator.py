# -*- coding: utf-8 -*-

import random
from queries.Query import dbQuery
import time
import config

import multiprocessing


class QueryGenerator:
    class replacePattern:
        def __init__(self, pattern, lambda_function):
            self._lambda_function = lambda_function
            self._pattern = pattern

        def replace(self, target, query_obj):
            return target.replace(self._pattern, self._lambda_function(query_obj))

    wild_card_rules = [
        replacePattern("<randInt>", lambda s: str(random.randint(1, 100000))),
        replacePattern("<randIntO7>", lambda s: str(random.randint(1, 10000000))),
        replacePattern("<randIntO6>", lambda s: str(random.randint(1, 1000000))),
        replacePattern("<randIntO5>", lambda s: str(random.randint(1, 100000))),
        replacePattern("<randIntO4>", lambda s: str(random.randint(1, 10000))),
        replacePattern("<randIntO3>", lambda s: str(random.randint(1, 1000))),
        replacePattern("<randIntO2>", lambda s: str(random.randint(1, 100))),
        replacePattern("<randIntO1>", lambda s: str(random.randint(1, 10))),
        replacePattern("<randInt2>", lambda s: str(random.randint(1, 100000))),
        replacePattern("<randInt3>", lambda s: str(random.randint(1, 100000))),
        replacePattern("<query_obj_id>", lambda s: str(s.query_id)),
        replacePattern("<non_uniform_rand_int_subscriber_size>", lambda s: str(QueryGenerator.non_uniform_random(1,
                                                                                                                 config.SUBSCRIBER_COUNT))),
        replacePattern("<rand_int_1_4>", lambda s: str(random.randint(1, 4))),
        replacePattern("<rand_0_8_16>", lambda s: str([0, 8, 24][random.randint(0, 2)])),
        replacePattern("<rand_1_to_24>", lambda s: str(random.randint(1, 24))),
        replacePattern("<bit_rand>", lambda s: str(random.randint(0, 1))),
        replacePattern("<rand_int_1_255>", lambda s: str(random.randint(1, 255))),
        replacePattern("<non_uniform_rand_int_subscriber_size_string>",
                       lambda s: str(QueryGenerator.non_uniform_random(1,
                                                                       config.SUBSCRIBER_COUNT)).rjust(15, '0')),
        replacePattern("<rand_int_1_big>", lambda s: str(random.randint(1, 256 * 256 * 256))),
    ]

    # Used for the TATP 
    @staticmethod
    def non_uniform_random(low, high):
        if config.USE_NON_UNIFORM_RANDOM:
            A = 2097151
            if config.SUBSCRIBER_COUNT >= 1000000:
                A = 65535
            if config.SUBSCRIBER_COUNT >= 10000000:
                A = 1048575
            return ((random.randint(0, A) | random.randint(low, high)) % (high - low + 1)) + low
        return random.randint(low, high)

    generator_id = 0

    def __init__(self, possible_query_list, need_to_parse, target_depth,
                 num_worker_threads, run_in_series, condition_variable):

        manager = multiprocessing.Manager()
        self.threads = []
        self.total_thread_count = num_worker_threads
        self.possible_query_list = possible_query_list
        self.generated_query_queue = manager.Queue()
        self.generator_id = QueryGenerator.generator_id
        QueryGenerator.generator_id = QueryGenerator.generator_id + 1

        for i in range(num_worker_threads):
            p = multiprocessing.Process(target=QueryGenerator.worker, args=(
                self.generated_query_queue, possible_query_list, need_to_parse, target_depth, run_in_series,
                QueryGenerator.generator_id, condition_variable))
            p.daemon = True
            p.start()
            self.threads.append(p)

    @staticmethod
    def pick_query_index_to_generate(possible_query_list):
        ticket_index = random.randint(0, 100)
        ticket_counter = 0
        for query_template_index in range(0, len(possible_query_list) / 2):
            ticket_counter += possible_query_list[(query_template_index * 2) + 1]
            if ticket_counter >= ticket_index:
                index = query_template_index * 2
                return index

    @staticmethod
    def generate_query(possible_query_list, index, generator_id, need_to_parse):
        query_text = possible_query_list[index]
        query = dbQuery(query_text, generator_id * 1000 + index)
        for replace_rule in QueryGenerator.wild_card_rules:
            query_text = replace_rule.replace(query_text, query)
        query.query_text = query_text
        if need_to_parse:
            query.parse()
        return query

    @staticmethod
    def worker(waiting_queue, possible_query_list, need_to_parse, target_depth, run_in_series, generator_id, cv):
        while True:
            with cv:
                cv.wait()
            while target_depth - waiting_queue.qsize() > 0:
                index = QueryGenerator.pick_query_index_to_generate(possible_query_list)
                last_query = QueryGenerator.generate_query(possible_query_list, index, generator_id, need_to_parse)
                waiting_queue.put(last_query)
                while run_in_series and not last_query.completed:
                    time.sleep(.001)
