# -*- coding: utf-8 -*-

import random
from time import sleep, time

from connectors.AbstractConnector import AbstractConnector
from queries.Query import dbQuery
import config

import multiprocessing

import cPickle
import zlib


class QueryGeneratorConnector(AbstractConnector):

    possible_query_sets =[]

    def __init__(self, received_queue, finished_list, policy):
        self.dibs_policy = policy
        self.condition_variable = multiprocessing.Condition()

        self.received_queue = received_queue

        self.threads = []
        self.total_thread_count = 0

        self.target_depth = config.DEFAULT_TARGET_DEPTH
        self.bundle_size = config.GENERATOR_BUNDLE_SIZE
        for i in range(config.DEFAULT_GENERATOR_WORKER_COUNT):
            self.add_generator()

        print("Prepopulating Generator Queue")
        while received_queue.qsize() * config.GENERATOR_BUNDLE_SIZE < self.target_depth:
            sleep(.01)
            self.notify_all()

    def next_queries(self):
        try:
            queries = self.received_queue.get(False)
            return cPickle.loads(zlib.decompress(queries))
        except multiprocessing.Queue.Empty:
            self.completed_all_queries()
            time.sleep(.1)
            return []
        self.notify_all()

    def end_processes(self):
        for p in self.threads:
            p.terminate()

    def completed_all_queries(self):
        print(" ### Not generating queries fast enough.")
        self.add_generator()

    def add_generator(self):
        p = multiprocessing.Process(target=QueryGenerator.worker, args=(
            self.received_queue, self.dibs_policy, self.target_depth, self.condition_variable, self.bundle_size))
        p.daemon = True
        p.start()
        self.total_thread_count += 1
        self.threads.append(p)

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

    # Notify all workers that we've used some queries
    def notify_all(self):
        for worker in self.threads:
            with self.condition_variable:
                self.condition_variable.notify()

    @staticmethod
    def pick_query_index_to_generate(possible_query_sets):
        ticket_index = random.randint(0, 100)
        ticket_counter = 0
        for query_template_index in range(0, len(possible_query_sets) / 2):
            ticket_counter += possible_query_sets[(query_template_index * 2) + 1]
            if ticket_counter >= ticket_index:
                index = query_template_index * 2
                return index

    @staticmethod
    def generate_query(possible_query_sets, index, dibs_policy):
        query_text = possible_query_sets[index]
        query = dbQuery(query_text, index)
        for replace_rule in QueryGenerator.wild_card_rules:
            query_text = replace_rule.replace(query_text, query)
        query.query_text = query_text
        dibs_policy.parse_query(query)
        return query

    @staticmethod
    def worker(waiting_queue, dibs_policy, target_depth, cv, bundle_size):
        while True:
            with cv:
                cv.wait()
            while target_depth - (waiting_queue.qsize()*bundle_size) > 0:
                query_bundle=[]
                for i in xrange(bundle_size):
                    index = QueryGenerator.pick_query_index_to_generate(QueryGenerator.possible_query_sets)
                    last_query = QueryGenerator.generate_query(QueryGenerator.possible_query_sets, index, dibs_policy)
                    query_bundle.append(last_query)
                query_bundle = zlib.compress(cPickle.dumps(query_bundle))
                waiting_queue.put(query_bundle)
