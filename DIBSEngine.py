import config
from clients.ClientManager import ClientConnectorManager
from isolation.IsolationManager import IsolationManager
import multiprocessing

import time

class DIBSEngine:
    @staticmethod
    def run(dibs_policy, client_connector_class, connector_class, seconds_to_run=10, worker_num=4, max_queries_total=10000):
        DIBSEngine.worker_num = worker_num

        print("Running for {} seconds with {} workers. In concurrency mode: {} ".format(seconds_to_run, worker_num,
                                                                                        str(dibs_policy)))

        ### Load Settings

        # Minimum queries in sidetrack to consider admitting
        min_queries_in_sidetrack = 0

        # Minimum queries to consider admitting from a sidetrack
        min_queries_from_sidetrack = 0
        # Maximum queries to leave in a sidetrack
        max_queries_from_sidetrack = 0

        bundle_size = 4

        manager = multiprocessing.Manager()
        incoming_queue = manager.Queue()
        complete_list = manager.list()

        print("Starting Connectors")
        connector = connector_class(incoming_queue, complete_list, dibs_policy)
        print("Connectors Started")

        query_completed_condition = multiprocessing.Condition()

        concurrency_engine = IsolationManager(dibs_policy,
                                              query_completed_condition,
                                              bundle_size,
                                              connector)

        concurrency_engine.append_next(config.MAX_QUERIES_IN_ENGINE)
        total_queries_admitted = 0
        print("Done Prepopulating Queue")

        ### Start client threads to push queries to the database
        clientManager = ClientConnectorManager(client_connector_class, worker_num, concurrency_engine.waiting_queries, concurrency_engine.completed_queries,
                                      query_completed_condition)

        start = time.time()

        while (True):
            if concurrency_engine.queries_left() < config.MAX_QUERIES_IN_ENGINE:
                queries_to_accept = config.MAX_QUERIES_IN_ENGINE - concurrency_engine.queries_left()

                # Don't go over max_queries_total when admitting more queries
                if queries_to_accept + total_queries_admitted > max_queries_total:
                    queries_to_accept = max_queries_total - total_queries_admitted

                # If we haven't hit max_queries_total, admit more queries
                if queries_to_accept > 0:
                    concurrency_engine.append_next(queries_to_accept)
                    total_queries_admitted = total_queries_admitted + queries_to_accept

            # Flag queries as complete - Can all be done at end for no-cc case.
            # concurrency_engine.proccess_completed_queries()

            if concurrency_engine.run_phased_policy:
                concurrency_engine.consider_changing_lock_mode(min_queries_in_sidetrack, min_queries_from_sidetrack,
                                                               max_queries_from_sidetrack)
            else:
                concurrency_engine.proccess_completed_queries()
                with query_completed_condition:
                    query_completed_condition.wait(.01)

            # If we're done, wrap up and print results.
            total_time = time.time() - start
            if (total_time > seconds_to_run) or (concurrency_engine.total_completed_queries() >= max_queries_total):

                print("Stopping Engine")

                print("  Stopping Clients")
                # End client threads sending queries to the database
                clientManager.end_processes()

                print("  Connectors Finishing Queries")

                # Process all completed queries
                concurrency_engine.proccess_completed_queries()

                print("  Stopping Connectors")

                connector.end_processes()

                break