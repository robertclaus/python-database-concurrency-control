
# Intended to be a generic multi-threaded way of taking dbQuery objects off of one queue, running them, and putting them onto another.  The more generic this stays, the better.  It should not be aware of locks or any of that.

import threading
import time
import MySQLdb

class dbClientManager:

    threads = []
    total_thread_count = 0

    waiting_queue = None
    complete_queue = None
    
    def __init__(self,num_worker_threads, waiting_queue, complete_queue):
        self.threads = []
        self.total_thread_count = num_worker_threads
        self.waiting_queue = waiting_queue
        self.complete_queue = complete_queue
        for i in range(num_worker_threads):
            conn = MySQLdb.connect(host='localhost',user='root',passwd='test',db='t')
            cur = conn.cursor()
            t = threading.Thread(target=dbClientManager.worker,
                                 args=[waiting_queue, complete_queue, conn, cur],)
            t.setDaemon(True)
            t.start()
            self.threads.append(t)

    def end_processes(self):
        # Place "Stop" commands (None) on worker queue
        for i in range(self.total_thread_count + 1):
            self.waiting_queue.put(None)
        
        # Burn off all tasks on the queue until we hit the "Stop" commands
        while self.waiting_queue.get() is not None:
            self.waiting_queue.task_done()
            pass
        
        # Wait for other threads to complete the remaining "Stop" commands
        self.waiting_queue.join()

    @staticmethod
    def worker(waiting_queue, complete_queue, connection, cursor):
        while True:
            query = waiting_queue.get()
            if query is None:
                break
            query.done_waiting()
            
            cursor.execute(query.query_text)
            connection.commit()
            complete_queue.put(query)
            query.complete()
            waiting_queue.task_done()
