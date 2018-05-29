# Intended to be a generic multi-threaded way of taking dbQuery objects off of one queue, running them, and putting them onto another.  The more generic this stays, the better.  It should not be aware of locks or any of that.

import time
from config import config

import multiprocessing

import MySQLdb
import psycopg2

from _mysql_exceptions import IntegrityError, OperationalError


class ClientManager:
    threads = []
    total_thread_count = 0

    waiting_queue = None
    complete_queue = None

    def __init__(self, num_worker_threads, waiting_queue, complete_queue, query_completed_condition, client_bundle_size):
        self.threads = []
        self.total_thread_count = num_worker_threads
        self.waiting_queue = waiting_queue
        self.complete_queue = complete_queue
        for i in range(num_worker_threads):
            p = multiprocessing.Process(target=ClientManager.worker,
                                        args=(waiting_queue, complete_queue, query_completed_condition, i, client_bundle_size))
            p.daemon = True
            p.start()
            self.threads.append(p)

    def end_processes(self):
        # Place "Stop" commands (None) on worker queue
        for i in range(self.total_thread_count + 2):
            self.waiting_queue.put(None)
        # Burn off all tasks on the queue until we hit the "Stop" commands
        while self.waiting_queue.get() is not None:
            self.waiting_queue.task_done()

        # Wait for other threads to complete the remaining "Stop" commands
        while any([thread for thread in self.threads if thread.is_alive()]):
            time.sleep(.01)

    @staticmethod
    def worker(waiting_queue, complete_queue, cv, worker_id, bundle_size):
        if config.DB_TYPE == config.DB_TYPE_MYSQL:
            connection = MySQLdb.connect(host=config.MYSQL_HOST, user=config.MYSQL_USER, passwd=config.MYSQL_PASSWORD,
                                         db=config.MYSQL_DB_NAME)
        if config.DB_TYPE == config.DB_TYPE_POSTGRES:
            connection = psycopg2.connect(database=config.POSTGRES_DB_NAME, user=config.POSTGRES_USER,
                                          password=config.POSTGRES_PASSWORD, host=config.POSTGRES_HOST,
                                          port=config.POSTGRES_PORT)
        cursor = connection.cursor()

        while True:
            start_wait = time.time()
            query_bundle = waiting_queue.get()
            end_wait = time.time()

            if query_bundle is None:
                connection.close()
                return

            for query in query_bundle:
                query.done_waiting()
                try:
                    cursor.execute(query.query_text)
                    connection.commit()
                except IntegrityError as IE:
                    query.log_error("Integrity")
                    pass
                except OperationalError as OE:
                    query.log_error("Operational (deadlock)")
                    pass
                query.complete()
                query.worker = worker_id
                query.worker_waited_time = end_wait - start_wait
                complete_queue.put(query)
                with cv:
                    cv.notify()
            waiting_queue.task_done()
