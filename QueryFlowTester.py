from clients.ClientManager import ClientManager
from isolation.ConcurrencyEngine import dbConcurrencyEngine
from connectors.QueryGenerator import QueryGenerator
from connectors import QuerySets
import multiprocessing

import time
import sys
import math

class QueryFlowTester:
    @staticmethod
    def run(run_concurrency_control=True, seconds_to_run=10, worker_num=4, max_queries_total=10000, query_set_choices=[]):

        def microseconds_used(sum, count, index):
            if index in sum and index in count:
                return str(1000000 * sum[index] / count[index])
            else:
                return '0'

        print("Running for {} seconds with {} workers. In concurrency mode: {} ".format(seconds_to_run, worker_num,
                                                                                        str(run_concurrency_control)))

        ### Load Settings

        # Minimum queries in incoming waiting query queue to allow before generating more
        min_queries_in_queue = worker_num * 1000

        # Minimum queries in sidetrack to consider admitting
        min_queries_in_sidetrack = 1000

        # Minimum queries to conisder admitting from a sidetrack
        min_queries_from_sidetrack = 100
        # Maximum queries to leave in a sidetrack
        max_queries_from_sidetrack = 0

        # Scheduling By Column Only, or also general predicate locking
        admit_to_sidetrack = run_concurrency_control

        # Maximum queries to have in the incoming generator queue at one time
        queue_depth = min_queries_in_queue*2  # *10

        # How many threads to have generating queries at a time
        generator_worker_num = worker_num * 10

        # Number of queries to pre-parse so queue does not start empty
        queries_to_start_in_queue_with = min_queries_in_queue

        bundle_size = 100

        # Load queries to generate.
        query_generator_condition = multiprocessing.Condition()  # Notifies the generator that we may have used some of its queries
        query_sets = QuerySets.query_sets
        query_generator_queues = []
        generator_processes = []
        for query_set_id in query_set_choices:
            query_set = query_sets[int(query_set_id)]
            # Create a thread to generate queries.  This is like an application submitting queries to the database.
            new_generator = QueryGenerator(query_set, run_concurrency_control, queue_depth, generator_worker_num,
                                           not query_set_id==query_set_choices[0],
                                           query_generator_condition, bundle_size)  # All but the first queryset wait for one query to complete before doing the next one.
            query_generator_queues.append(new_generator.generated_query_queue)
            generator_processes.append(new_generator)


        ### Pre-generate query queues and admit some queries
        print("Prepopulating Queue")
        while query_generator_queues[0].qsize()*bundle_size < queue_depth:
            time.sleep(.01)
            with query_generator_condition:
                query_generator_condition.notify()
        print("  Query Generators done.  Waiting for CC Startup")

        query_completed_condition = multiprocessing.Condition()

        concurrency_engine = dbConcurrencyEngine(query_generator_queues,
                                                 query_generator_condition,
                                                 run_concurrency_control,
                                                 query_completed_condition,
                                                 bundle_size,
                                                 bundle_size,
                                                 generator_worker_num)

        concurrency_engine.append_next(queries_to_start_in_queue_with)
        total_queries_admitted = queries_to_start_in_queue_with
        print("Done Prepopulating Queue")

        ### Start client threads to push queries to the database
        clientManager = ClientManager(worker_num, concurrency_engine.waiting_queries, concurrency_engine.completed_queries,
                                      query_completed_condition, bundle_size)

        start = time.time()

        loop_count = 0;

        while (True):
            if concurrency_engine.queries_left() < min_queries_in_queue:
                queries_to_accept = min_queries_in_queue - concurrency_engine.queries_left()

                # Don't go over max_queries_total when admitting more queries
                if queries_to_accept + total_queries_admitted > max_queries_total:
                    queries_to_accept = max_queries_total - total_queries_admitted

                # If we haven't hit max_queries_total, admit more queries
                if queries_to_accept > 0:
                    concurrency_engine.append_next(queries_to_accept)
                    total_queries_admitted = total_queries_admitted + queries_to_accept

            # Flag queries as complete - Can all be done at end for no-cc case.
            # concurrency_engine.proccess_completed_queries()

            if run_concurrency_control:
                concurrency_engine.consider_changing_lock_mode(min_queries_in_sidetrack, min_queries_from_sidetrack,
                                                               max_queries_from_sidetrack)
            else:
                with query_completed_condition:
                    query_completed_condition.wait(.01)

            # If we're done, wrap up and print results.
            total_time = time.time() - start
            if (total_time > seconds_to_run) or (concurrency_engine.total_completed_queries() >= max_queries_total):

                print("Done")

                # End client threads sending queries to the database
                clientManager.end_processes()

                print("Clients Stopped")

                # Process all completed queries
                concurrency_engine.proccess_completed_queries()

                print("Completed Queries Archived")

                # Print any data that might be interesting (primarily concurrency_engine._archive_completed_queries)
                type_index_sum = {}
                type_index_count = {}
                std_devs = {}
                max = {}
                admit_time = {}
                total_wait_time = 0

                completed = len(concurrency_engine._archive_completed_queries)


                for query in concurrency_engine._archive_completed_queries:
                    if not query.query_type_id in type_index_sum:
                        type_index_sum[query.query_type_id] = 0
                    type_index_sum[query.query_type_id] += query.total_time - query.waiting_time

                    if not query.query_type_id in type_index_count:
                        type_index_count[query.query_type_id] = 0
                    type_index_count[query.query_type_id] += 1
                    if query.worker_waited_time is not None:
                        total_wait_time += query.worker_waited_time

                for query in concurrency_engine._archive_completed_queries:
                    if not query.query_type_id in std_devs:
                        std_devs[query.query_type_id] = 0
                    mean = type_index_sum[query.query_type_id] / type_index_count[query.query_type_id]
                    deviation = mean - (query.total_time - query.waiting_time)
                    std_devs[query.query_type_id] += deviation * deviation

                for query in concurrency_engine._archive_completed_queries:
                    if not query.query_type_id in admit_time:
                        admit_time[query.query_type_id] = 0
                    if query.time_to_admit > admit_time[query.query_type_id]:
                        admit_time[query.query_type_id] = query.time_to_admit

                for query in concurrency_engine._archive_completed_queries:
                    if not query.query_type_id in max:
                        max[query.query_type_id] = 0
                    if query.total_time - query.waiting_time > max[query.query_type_id]:
                        max[query.query_type_id] = query.total_time - query.waiting_time

                with open('allqueries.csv', 'wb') as file:
                    for query in concurrency_engine._archive_completed_queries:
                        file.write("{},{},{},{},{},\"{}\"\n".format(query.id, query.worker,
                                                                    1000 * (query.waiting_time + query.created_at),
                                                                    1000 * (query.total_time + query.created_at),
                                                                    query.lock_run_under, query.query_type_id))

                # Total utilization
                total_time_executing = 0
                for query_id in type_index_sum:
                    total_time_executing += type_index_sum[query_id]
                total_utilization = (total_time_executing / worker_num) / total_time

                for query_id in type_index_sum:
                    print("Type [{}] Count: {} Average Execution Time: {} [admit[{:1f}] max[{:1f}] +/- {:1f}]".format(
                        str(query_id), str(type_index_count[query_id]),
                        str(type_index_sum[query_id] / type_index_count[query_id]), admit_time[query_id], max[query_id],
                        math.sqrt(std_devs[query_id])))
                print("Average Worker Wait Time: {}".format(total_wait_time / worker_num))
                print("Time spent processing completed queries {}".format(concurrency_engine.time_processing_completed))
                print("Total Time: {}".format(total_time))
                print("Number of scheduling cycles: {}".format(concurrency_engine.cycle_count))
                print("Completed: {}".format(completed))
                print("Utilization %: {}".format(total_utilization * 100))
                if total_utilization < .98:
                    print("### ERROR: Utilization under 98% - Indicates this process was too slow.")
                print("Throughput (Q/s) : " + str(completed / total_time))

                sys.stdout.write(
                    "\n csv,{},{},{},{}".format(total_time, worker_num, str(completed / total_time), total_utilization * 100))
                for query_type in type_index_sum:
                    sys.stdout.write(",{},{},{}".format(query_type, microseconds_used(type_index_sum, type_index_count, 1000),
                                                        type_index_count))
                break