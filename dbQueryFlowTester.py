from dbClientManager import dbClientManager
from dbConcurrencyEngine import dbConcurrencyEngine
from dbQueryGenerator import dbQueryGenerator
import dbQuerySets
import threading
import multiprocessing

import time
import sys
import math

### Primary python script for testing queries with or without external concurrency control.

### Load all params from command line

seconds_to_run = 10
worker_num = 10
run_concurrency_control = True
max_queries_total = 10000

if len(sys.argv)>1:
    run_concurrency_control = (sys.argv[1] == "1")
if len(sys.argv)>2:
    seconds_to_run = int(sys.argv[2])
if len(sys.argv)>3:
    worker_num = int(sys.argv[3])
if len(sys.argv)>4:
    max_queries_total = int(sys.argv[4])

param_that_starts_query_sets = 5

print("Running for {} seconds with {} workers. In concurrency mode: {} ".format(seconds_to_run, worker_num, str(run_concurrency_control)))


### Load Settings

# Minimum queries in incoming query queue to allow before generating more
min_queries_in_queue = worker_num*100 #*4

# Maximum queries to have in the incoming query queue at one time
queue_depth = worker_num*200

# How many queries to admit from the incoming query queue into the system
queries_to_accept_at_a_time = worker_num*200

# How many threads to have generating queries at a time
generator_worker_num= worker_num*8

# Number of queries to pre-parse so queue does not start empty
queries_to_start_in_queue_with = min_queries_in_queue

# Load queries to generate.
query_generator_condition = multiprocessing.Condition() # Notifies the generator that we may have used some of its queries
query_sets = dbQuerySets.query_sets
query_generator_queues =[]
for query_set_id in sys.argv[param_that_starts_query_sets:]:
    query_set = query_sets[int(query_set_id)]
    # Create a thread to generate queries.  This is like an application submitting queries to the database.
    new_generator = dbQueryGenerator(query_set, run_concurrency_control, queue_depth, generator_worker_num, not (int(query_set_id)==int(sys.argv[param_that_starts_query_sets])),query_generator_condition) # All but the first queryset wait for one query to complete before doing the next one.
    query_generator_queues.append(new_generator.generated_query_queue)


### Pre-generate query queues and admit some queries
print("Prepopulating Queue")
while query_generator_queues[0].qsize()<min_queries_in_queue:
    time.sleep(.01)
    with query_generator_condition:
      query_generator_condition.notify()
print("  Query Generators done.  Waiting for CC Startup")
query_completed_condition = threading.Condition()
concurrency_engine = dbConcurrencyEngine(query_generator_queues, query_generator_condition)

concurrency_engine.append_next(queries_to_start_in_queue_with, run_concurrency_control)
total_queries_admitted = queries_to_start_in_queue_with
print("Done Prepopulating Queue")

### Start client threads to push queries to the database
clientManager = dbClientManager(worker_num, concurrency_engine.waiting_queries, concurrency_engine.completed_queries, query_completed_condition)

start = time.time()

loop_count = 0;

while(True):
    if concurrency_engine.queries_left() < min_queries_in_queue:
        # Don't go over max_queries_total when admitting more queries
        if queries_to_accept_at_a_time + total_queries_admitted > max_queries_total:
            queries_to_accept_at_a_time = max_queries_total - total_queries_admitted

        # Flag queries as complete
        concurrency_engine.proccess_completed_queries()

        # Try admitting sidetracked queries first so those get priority over new ones
        concurrency_engine.move_sidetracked_queries(min_queries_in_queue)

        # If we won't hit max_queries_total, admit more queries
        if queries_to_accept_at_a_time > 0:
            # Admit queries faster if the queue is close to empty
            if concurrency_engine.waiting_queries.qsize()<1:
                #queries_to_accept_at_a_time = queries_to_accept_at_a_time*2
                #print(" ######### NOT ACCEPTING QUERIES FAST ENOUGH: {}".format(concurrency_engine.waiting_queries.qsize()))
                pass
            concurrency_engine.append_next(queries_to_accept_at_a_time, run_concurrency_control)
            total_queries_admitted = total_queries_admitted + queries_to_accept_at_a_time
    elif len(concurrency_engine._sidetracked_query_list) > 0:
        # Flag queries as complete
        concurrency_engine.proccess_completed_queries()

        # Try admitting sidetracked queries first so those get priority over new ones
        concurrency_engine.move_sidetracked_queries(min_queries_in_queue)

    with query_completed_condition:
      query_completed_condition.wait(.1)

    # If we're done, wrap up and print results.
    total_time = time.time() - start
    if (total_time > seconds_to_run) or (concurrency_engine.total_completed_queries()>=max_queries_total):

        print("Done")
        
        # End client threads sending queries to the database
        clientManager.end_processes()
        
        # Process all completed queries
        concurrency_engine.proccess_completed_queries()
        
        # Print any data that might be interesting (primarily concurrency_engine._archive_completed_queries)
        type_index_sum={}
        type_index_count={}
        std_devs={}
        total_wait_time=0
        
        completed = concurrency_engine.total_completed_queries()
        
        sidetracked = len(concurrency_engine._sidetracked_query_list)
        
        for query in concurrency_engine._archive_completed_queries:
            if not query.query_type_id in type_index_sum:
                type_index_sum[query.query_type_id]=0
            type_index_sum[query.query_type_id] += query.total_time - query.waiting_time
            
            if not query.query_type_id in type_index_count:
                type_index_count[query.query_type_id] =0
            type_index_count[query.query_type_id] += 1

            total_wait_time += query.waiting_time
        
        for query in concurrency_engine._archive_completed_queries:
            if not query.query_type_id in std_devs:
                std_devs[query.query_type_id] = 0
            mean = type_index_sum[query.query_type_id]/type_index_count[query.query_type_id]
            deviation = mean - (query.total_time - query.waiting_time)
            std_devs[query.query_type_id] += deviation * deviation

        # Total utilization
        total_time_executing = 0
        for query_id in type_index_sum:
          total_time_executing += type_index_sum[query_id]
        total_utilization = (total_time_executing / worker_num) / total_time

        for query_id in type_index_sum:
            print("Type [{}] Count: {} Average Execution Time: {} [+/- {:1f} ]".format(str(query_id),str(type_index_count[query_id]), str(type_index_sum[query_id]/type_index_count[query_id]), math.sqrt(std_devs[query_id])))
        print("Average External Wait : {}".format(str(total_wait_time/concurrency_engine.total_completed_queries())))
        print("Total Time: {}".format(total_time))
        print("Completed: "+str(concurrency_engine.total_completed_queries()))
        print("Sidetracked: "+str(len(concurrency_engine._sidetracked_query_list)))
        print("Utilization %: {}".format(total_utilization*100))
        if total_utilization < .98:
          print("### ERROR: Utilization under 98% - Indicates this process was too slow.")
        print("Throughput (Q/s) : " + str(completed/total_time))

        if sys.argv[param_that_starts_query_sets] == '12':
          print("{},{},{},{},{},{},{}, {},{},{}".format(total_time, worker_num, str(completed/total_time),
                                    str(1000000*type_index_sum[1000]/type_index_count[1000]),
                                    str(1000000*type_index_sum[1002]/type_index_count[1002]),
                                    str(1000000*type_index_sum[1004]/type_index_count[1004]),
                                    str(1000000*type_index_sum[1006]/type_index_count[1006]),
                                    str(1000000*type_index_sum[1008]/type_index_count[1008]),
                                    str(1000000*type_index_sum[1010]/type_index_count[1010]),
                                    str(1000000*type_index_sum[1012]/type_index_count[1012]),
                                    ))
        break

sys.exit()
