import config
from clients.ClientConnectorManager import ClientConnectorManager
from isolation.IsolationManager import IsolationManager
import multiprocessing

import time

class DIBSEngine:
    @staticmethod
    def run(dibs_policy, client_connector_class, connector_class):
        DIBSEngine.worker_num = config.NUMBER_OF_DATABASE_CLIENTS
        DIBSEngine.max_queries_total = config.MAX_QUERIES_TO_RUN
        DIBSEngine.max_seconds_to_run = config.MAX_SECONDS_TO_RUN

        print("Running for {} seconds with {} workers. In concurrency mode: {} ".format(DIBSEngine.max_seconds_to_run,
                                                                                        DIBSEngine.worker_num,
                                                                                        str(dibs_policy)))

        ### Load Settings

        # Minimum queries in sidetrack to consider admitting
        min_queries_in_sidetrack = config.MIN_QUERIES_IN_SIDETRACK

        # Minimum queries to consider admitting from a sidetrack
        min_queries_from_sidetrack = config.MIN_QUERIES_FROM_SIDETRACK
        # Maximum queries to leave in a sidetrack
        max_queries_from_sidetrack = config.MAX_QUERIES_FROM_SIDETRACK

        manager = multiprocessing.Manager()
        incoming_queue = manager.Queue()
        complete_list = manager.list()

        print("Starting Connectors")
        connector = connector_class(incoming_queue, complete_list, dibs_policy)
        print("Connectors Started")

        query_completed_condition = multiprocessing.Condition()

        isolation_engine = IsolationManager(dibs_policy,
                                              query_completed_condition,
                                              connector)

        isolation_engine.append_next(config.MAX_QUERIES_IN_ENGINE)
        total_queries_admitted = 0
        print("Done Prepopulating Queue")

        ### Start client threads to push queries to the database
        clientManager = ClientConnectorManager(client_connector_class,
                                               isolation_engine.waiting_queries,
                                               isolation_engine.completed_queries,
                                               query_completed_condition)

        start = time.time()

        while (True):
            if isolation_engine.queries_left() < config.MAX_QUERIES_IN_ENGINE:
                queries_to_accept = config.MAX_QUERIES_IN_ENGINE - isolation_engine.queries_left()

                # Don't go over max_queries_total when admitting more queries
                if queries_to_accept + total_queries_admitted > DIBSEngine.max_queries_total:
                    queries_to_accept = DIBSEngine.max_queries_total - total_queries_admitted

                # If we haven't hit max_queries_total, admit more queries
                if queries_to_accept > 0:
                    isolation_engine.append_next(queries_to_accept)
                    total_queries_admitted = total_queries_admitted + queries_to_accept

            # Flag queries as complete - Can all be done at end for no-cc case.
            # isolation_engine.proccess_completed_queries()

            isolation_engine.proccess_completed_queries()
            with query_completed_condition:
                query_completed_condition.wait(.01)

            # If we're done, wrap up and print results.
            total_time = time.time() - start
            if (total_time > DIBSEngine.max_seconds_to_run) or (isolation_engine.total_completed_queries() >= DIBSEngine.max_queries_total):

                print("Stopping Engine")

                print("  Stopping Clients")
                # End client threads sending queries to the database
                clientManager.end_processes()

                print("  Connectors Finishing Queries")

                # Process all completed queries
                isolation_engine.proccess_completed_queries()

                print("  Stopping Connectors")

                connector.end_processes()

                break