# -*- coding: utf-8 -*-
import math
import os
import random
import sys
from time import sleep, time

from DIBSEngine import DIBSEngine
from connectors.AbstractConnector import AbstractConnector
from isolation.IsolationManager import IsolationManager
from policies.PhasedPolicy import PhasedPolicy
from queries.Query import dbQuery
import config

import multiprocessing
from Queue import Empty

import cPickle
import zlib

class QueryGeneratorConnector(AbstractConnector):

    possible_query_sets =[]
    last_isolation_level = None

    def __init__(self, policy):
        self.finished_list = []
        self.dibs_policy = policy
        self.condition_variable = multiprocessing.Condition()

        self.total_query_sizes = 0
        self.total_query_count = 0

        manager = multiprocessing.Manager()
        self.received_queue = manager.Queue()

        self.threads = []
        self.total_thread_count = 0

        self.target_depth = config.DEFAULT_TARGET_DEPTH
        self.bundle_size = config.GENERATOR_BUNDLE_SIZE
        for i in range(config.DEFAULT_GENERATOR_WORKER_COUNT):
            self.add_generator()

        print("Prepopulating Generator Queue")
        while self.received_queue.qsize() * config.GENERATOR_BUNDLE_SIZE < self.target_depth:
            sleep(.1)
            self.notify_all()
            self.add_generator()


    def next_queries(self):
        self.notify_all()
        try:
            pickled_queries = self.received_queue.get_nowait()
            unpickled_queries = cPickle.loads(zlib.decompress(pickled_queries))
            decompressed_unpickled_queries = [dbQuery.decompress(q) for q in unpickled_queries]
            self.total_query_sizes += len(pickled_queries)
            self.total_query_count += self.bundle_size
            return decompressed_unpickled_queries
        except Empty:
            self.add_generator()
            sleep(.05)
            return []

    def complete_query(self, query):
        self.finished_list.append(query)

    def end_processes(self):
        self.terminate_all()
        self.print_stats()

    def terminate_all(self):
        for p in self.threads:
            p.terminate()

    def add_generator(self):
        if self.total_thread_count < config.MAX_GENERATORS:
            p = multiprocessing.Process(target=QueryGeneratorConnector.worker, args=(
                self.received_queue, self.dibs_policy, self.target_depth, self.condition_variable, self.bundle_size))
            p.daemon = True
            p.start()
            self.total_thread_count += 1
            self.threads.append(p)
        else:
            print("Not generating queries fast enough.")

    def print_stats(self):

        def microseconds_used(sum, count, index):
            if index in sum and index in count:
                return str(1000000 * sum[index] / count[index])
            else:
                return '0'

        end_time = -1
        for query in self.finished_list:
            if query.completed_at > end_time:
                end_time = query.completed_at

        start_time = self.finished_list[0].start_admit_time
        for query in self.finished_list:
            if query.start_admit_time < start_time:
                start_time = query.start_admit_time

        total_time = end_time - start_time

        # Print any data that might be interesting (primarily concurrency_engine._archive_completed_queries)
        type_index_sum = {}
        type_index_count = {}
        std_devs = {}
        max = {}
        admit_time = {}
        total_wait_time = 0

        finished_list = self.finished_list

        completed = len(finished_list)

        for query in finished_list:
            if not query.query_type_id in type_index_sum:
                type_index_sum[query.query_type_id] = 0
            type_index_sum[query.query_type_id] += query.total_time - query.waiting_time

            if not query.query_type_id in type_index_count:
                type_index_count[query.query_type_id] = 0
            type_index_count[query.query_type_id] += 1
            if query.worker_waited_time is not None:
                total_wait_time += query.worker_waited_time

        for query in finished_list:
            if not query.query_type_id in std_devs:
                std_devs[query.query_type_id] = 0
            mean = type_index_sum[query.query_type_id] / type_index_count[query.query_type_id]
            deviation = mean - (query.total_time - query.waiting_time)
            std_devs[query.query_type_id] += deviation * deviation

        for query in finished_list:
            if not query.query_type_id in admit_time:
                admit_time[query.query_type_id] = 0
            if query.time_to_admit > admit_time[query.query_type_id]:
                admit_time[query.query_type_id] = query.time_to_admit

        for query in finished_list:
            if not query.query_type_id in max:
                max[query.query_type_id] = 0
            if query.total_time - query.waiting_time > max[query.query_type_id]:
                max[query.query_type_id] = query.total_time - query.waiting_time

        with open('allqueries.csv', 'wb') as file:
            for query in finished_list:
                file.write("{},{},{},{},{},\"{}\"\n".format(query.id, query.worker,
                                                            1000 * (query.waiting_time + query.created_at),
                                                            1000 * (query.total_time + query.created_at),
                                                            query.lock_run_under, query.query_type_id))

        # Total utilization
        total_time_executing = 0
        for query_id in type_index_sum:
            total_time_executing += type_index_sum[query_id]
        total_utilization = (total_time_executing / DIBSEngine.worker_num) / total_time
        total_utilization = 1 - ((total_wait_time/DIBSEngine.worker_num) / total_time)
        for query_id in type_index_sum:
            print("Type [{}] Count: {} [{:d}%] Average Execution Time: {} [admit[{:1f}] max[{:1f}] +/- {:1f}]".format(
                str(query_id), str(type_index_count[query_id]), 100*type_index_count[query_id]/completed,
                str(type_index_sum[query_id] / type_index_count[query_id]), admit_time[query_id], max[query_id],
                math.sqrt(std_devs[query_id])))
        print("Average Worker Wait Time: {}".format(total_wait_time / DIBSEngine.worker_num))
        print("Time spent processing completed queries {}".format(IsolationManager.time_processing_completed))
        print("Average Query Size From Generator: {}".format(self.total_query_sizes/self.total_query_count))
        print("Total Generator Count: {}".format(self.total_thread_count))
        print("Total Time: {}".format(total_time))
        print("Completed: {}".format(completed))
        print("Utilization %: {}".format(total_utilization * 100))
        if total_utilization < .98:
            print("### ERROR: Utilization under 98% - Indicates this process was too slow.")
        print("Throughput (Q/s) : " + str(completed / total_time))

        sys.stdout.write(
            "\n csv,{},{},{},{},{}".format(total_time, DIBSEngine.worker_num, completed, str(completed / total_time),
                                        total_utilization * 100))

        for query_type in sorted(type_index_sum.iterkeys()):
            sys.stdout.write(",{}".format(microseconds_used(type_index_sum, type_index_count, query_type)))
        for query_type in sorted(type_index_sum.iterkeys()):
            sys.stdout.write(",{}".format(type_index_count[query_type]))
        for query_type in sorted(type_index_sum.iterkeys()):
            sys.stdout.write(
                ",{},{},{}".format(query_type, microseconds_used(type_index_sum, type_index_count, query_type),
                                   type_index_count[query_type]))

        sys.stdout.write(", {}, {}, {} \n\n\n\n".format(self.last_isolation_level, DIBSEngine.worker_num, QueryGeneratorConnector.possible_query_sets))

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
        replacePattern("<non_uniform_rand_int_subscriber_size>", lambda s: str(QueryGeneratorConnector.non_uniform_random(1,
                                                                                                                 config.SUBSCRIBER_COUNT))),
        replacePattern("<rand_int_1_4>", lambda s: str(random.randint(1, 4))),
        replacePattern("<rand_0_8_16>", lambda s: str([0, 8, 24][random.randint(0, 2)])),
        replacePattern("<rand_1_to_24>", lambda s: str(random.randint(1, 24))),
        replacePattern("<bit_rand>", lambda s: str(random.randint(0, 1))),
        replacePattern("<rand_int_1_255>", lambda s: str(random.randint(1, 255))),
        replacePattern("<non_uniform_rand_int_subscriber_size_string>",
                       lambda s: str(QueryGeneratorConnector.non_uniform_random(1,
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
        for replace_rule in QueryGeneratorConnector.wild_card_rules:
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
                    index = QueryGeneratorConnector.pick_query_index_to_generate(QueryGeneratorConnector.possible_query_sets)
                    last_query = QueryGeneratorConnector.generate_query(QueryGeneratorConnector.possible_query_sets, index, dibs_policy)
                    last_query.ps_id = os.getpid()
                    compresed_query = last_query.compress()
                    query_bundle.append(compresed_query)
                query_bundle = zlib.compress(cPickle.dumps(query_bundle))
                waiting_queue.put(query_bundle)
