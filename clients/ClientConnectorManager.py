import multiprocessing
import time
import config

class ClientConnectorManager:

    def __init__(self,client_connector, waiting_queue, complete_queue, query_completed_condition):
        self.client_connector = client_connector
        self.threads = []
        self.waiting_queue = waiting_queue
        self.complete_queue = complete_queue
        self.query_completed_condition = query_completed_condition
        self.next_process_id = 1
        for i in range(config.NUMBER_OF_DATABASE_CLIENTS):
            self.add_process()

    def add_process(self):
        p = multiprocessing.Process(target=ClientConnectorManager.worker,
                                    args=(self.client_connector,
                                          self.waiting_queue,
                                          self.complete_queue,
                                          self.query_completed_condition,
                                          self.next_process_id)
                                    )
        self.next_process_id += 1
        self.threads.append(p)

        p.daemon = True
        p.start()

    @staticmethod
    def worker(client_connector, waiting_queue, complete_queue, cv, worker_id):
        connector = client_connector()

        while True:
            start_wait = time.time()
            query_bundle = waiting_queue.get()
            end_wait = time.time()

            for query in query_bundle:
                query.waiting_time = time.time() - query.created_at

                try:
                    query.result = connector.execute(query.query_text)

                except Exception as e:
                    query.error = "ERROR: {}".format(str(e))

                query.completed_at = time.time()
                query.total_time = query.completed_at - query.created_at
                query.worker = worker_id
                query.worker_waited_time = (end_wait - start_wait)/len(query_bundle)
                complete_queue.put(query)
                with cv:
                    cv.notify()
            waiting_queue.task_done()

    def end_processes(self):
        for i in self.threads:
            i.terminate()