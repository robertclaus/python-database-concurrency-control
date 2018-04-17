
# Intended to be a generic multi-threaded way of taking dbQuery objects off of one queue, running them, and putting them onto another.  The more generic this stays, the better.  It should not be aware of locks or any of that.

import threading
import time
from config import config

import MySQLdb
import psycopg2

from _mysql_exceptions import IntegrityError, OperationalError

class dbClientManager:

    threads = []
    total_thread_count = 0

    waiting_queue = None
    complete_queue = None
    
    def __init__(self,num_worker_threads, waiting_queue, complete_queue, query_completed_condition):
        self.threads = []
        self.total_thread_count = num_worker_threads
        self.waiting_queue = waiting_queue
        self.complete_queue = complete_queue
        for i in range(num_worker_threads):
          if config.DB_TYPE == config.DB_TYPE_MYSQL:
            conn = MySQLdb.connect(host=config.MYSQL_HOST,user=config.MYSQL_USER,passwd=config.MYSQL_PASSWORD,db=config.MYSQL_DB_NAME)
          if config.DB_TYPE == config.DB_TYPE_POSTGRES:
            conn = psycopg2.connect(database = config.POSTGRES_DB_NAME, user = config.POSTGRES_USER, password = config.POSTGRES_PASSWORD, host = config.POSTGRES_HOST, port = config.POSTGRES_PORT)
          cur = conn.cursor()


          t = threading.Thread(target=dbClientManager.worker,
                               args=[waiting_queue, complete_queue, conn, cur, query_completed_condition],)
          t.setDaemon(True)
          t.start()
          self.threads.append(t)

    def end_processes(self):
        # Place "Stop" commands (None) on worker queue
        for i in range(self.total_thread_count + 2):
            self.waiting_queue.put(None)
        # Burn off all tasks on the queue until we hit the "Stop" commands
        while self.waiting_queue.get() is not None:
            self.waiting_queue.task_done()
        
        # Wait for other threads to complete the remaining "Stop" commands
        while any([thread for thread in self.threads if thread.isAlive()]):
            time.sleep(.01)

    @staticmethod
    def worker(waiting_queue, complete_queue, connection, cursor, cv):
        while True:
            query = waiting_queue.get()
            if query is None:
                connection.close()
                return
            query.done_waiting()
            try:
              cursor.execute(query.query_text)
              connection.commit()
            except IntegrityError as IE:
              pass
              #print("Integrity Error")
            except OperationalError as OE:
              pass
              #print("Operational Error (Deadlock)")
            complete_queue.put(query)
            
            query.complete()
            waiting_queue.task_done()
            with cv:
              cv.notifyAll()
