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

        print("Running for {} seconds or {} queries with {} workers. "
              "In concurrency mode: {} ".format(DIBSEngine.max_seconds_to_run,
                                                DIBSEngine.max_queries_total,
                                                DIBSEngine.worker_num,
                                                str(dibs_policy)))

        print("  Starting Connectors")
        connector = connector_class(dibs_policy)

        query_completed_condition = multiprocessing.Condition()

        print("  Starting Isolation Engine")
        isolation_engine = IsolationManager(dibs_policy, connector)

        total_queries_admitted = 0
        print("  Starting Database Clients")

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
                if queries_to_accept + total_queries_admitted >= DIBSEngine.max_queries_total:
                    queries_to_accept = DIBSEngine.max_queries_total - total_queries_admitted

                # If we haven't hit max_queries_total, admit more queries
                if queries_to_accept > 0:
                    isolation_engine.append_next(queries_to_accept)
                    total_queries_admitted = total_queries_admitted + queries_to_accept

            isolation_engine.proccess_completed_queries()
            with query_completed_condition:
                query_completed_condition.wait(.01)

            # If we're done, wrap up and print results.
            total_time = time.time() - start
            if (total_time > DIBSEngine.max_seconds_to_run) or \
                    (isolation_engine.total_completed_queries() >= DIBSEngine.max_queries_total):

                print("Stopping Engine")

                print("  Stopping Clients")
                # End client threads sending queries to the database
                clientManager.end_processes()

                print("  Connectors Clearing Completed Queries")

                # Process all completed queries
                isolation_engine.proccess_completed_queries()

                print("  Stopping Connectors")

                connector.end_processes()

                print("Engine Stopped")
                break