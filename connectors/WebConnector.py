import multiprocessing

from connectors.AbstractConnector import AbstractConnector
from BaseHTTPServer import BaseHTTPRequestHandler
from urlparse import urlparse, parse_qs
from Queue import Empty

from SocketServer import TCPServer

from queries.Query import dbQuery


class WebConnector(AbstractConnector):
    def __init__(self, received_queue, finished_list, policy):
        self.received_queue = received_queue
        self.finished_list = finished_list
        self.policy = policy
        p = multiprocessing.Process(target=WebConnector.worker, args=(received_queue, finished_list, policy))
        p.daemon = True
        p.start()
        self.process = p

    def end_processes(self):
        self.process.terminate()

    def next_queries(self):
        try:
            query = self.received_queue.get(False)
            return [query]
        except Empty:
            return []


    @staticmethod
    def worker(received_queue, finished_list, policy):
        RequestHandler.received_queue = received_queue
        RequestHandler.finished_list = finished_list
        RequestHandler.policy = policy

        try:
            httpd = TCPServer(("", 8000), RequestHandler)
            print("Serving at port", 8000)
            httpd.serve_forever()
        except:
            print("Server Error")



class RequestHandler(BaseHTTPRequestHandler):
    received_queue = None
    finished_list = None
    policy = None

    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        query_components = parse_qs(urlparse(self.path).query)
        if "query_text" in query_components:
            query_text = query_components["query_text"]
            print("Query Text: {}".format(query_text))

            # Parse and submit the query
            query = dbQuery(query_text, 1)
            RequestHandler.policy.parse_query(query)
            RequestHandler.received_queue.put(query)

            self._set_headers()
            self.wfile.write("<html><body>Submitted Query With ID {}</body></html>\n".format(query.query_id))
            return

        if "query_id" in query_components:
            # Check if query is in finished_list and return it
            query_id = query_components["query_id"]
            print("Finished list contains {} entries.".format(len(RequestHandler.finished_list)))
            print("Searching for entry [{}]".format(query_id))
            query = next((q for q in RequestHandler.finished_list if q.query_id == query_id), None)
            self._set_headers()
            if query:
                self.wfile.write("<html><body>Query Competed With Results: {}</body></html>".format(query.result))
            else:
                self.wfile.write("<html><body>Query not complete.</body></html>\n")
            return

        else:
            self._set_headers()
            self.wfile.write("<html><body>Submit GET with query_text parameter to submit a query or query_id parameter"
                             "to retrieve results.</body></html>\n")

    def do_HEAD(self):
        self._set_headers()