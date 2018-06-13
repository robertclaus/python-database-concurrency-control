import multiprocessing

from connectors.AbstractConnector import AbstractConnector
from BaseHTTPServer import BaseHTTPRequestHandler
from urlparse import urlparse, parse_qs
from Queue import Empty

from SocketServer import TCPServer

from queries.Query import dbQuery


class WebConnector(AbstractConnector):
    def __init__(self, received_queue, finished_list, policy):
        WebConnector.received_queue = received_queue
        WebConnector.finished_list = finished_list
        WebConnector.policy = policy
        p = multiprocessing.Process(target=WebConnector.worker, args=())
        p.daemon = True
        p.start()
        self.process = p

    def end_processes(self):
        self.process.terminate()

    def next_queries(self):
        try:
            query = WebConnector.received_queue.get_nowait()
            return [query]
        except Empty:
            return []

    def complete_query(self, query):
        self.finished_list.append(query)

    @staticmethod
    def worker():
        try:
            httpd = TCPServer(("", 8000), RequestHandler)
            print("Serving at port", 8000)
            httpd.serve_forever()
        except:
            print("Server Error")



class RequestHandler(BaseHTTPRequestHandler):

    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        query_components = parse_qs(urlparse(self.path).query)
        if "query_text" in query_components:
            query_text = " ".join(query_components["query_text"])
            print("Query Text: {}".format(query_text))

            # Parse and submit the query
            query = dbQuery(query_text, 1)
            WebConnector.policy.parse_query(query)
            WebConnector.received_queue.put(query)

            self._set_headers()
            self.wfile.write("<html><body>Submitted Query With ID {}</body></html>\n".format(query.query_id))
            return

        if "query_id" in query_components:
            # Check if query is in finished_list and return it
            query_id = int(query_components["query_id"][0])
            print("Finished list contains {} entries.".format(len(WebConnector.finished_list)))
            print("Searching for entry [{}]".format(query_id))
            query = next((q for q in WebConnector.finished_list if q.query_id == query_id), None)
            self._set_headers()
            if query:
                if query.result:
                    self.wfile.write("<html><body>Query Competed With Results: {}</body></html>".format(query.result))
                    return
                if query.error:
                    self.wfile.write("<html><body>Query Competed With Error: {}</body></html>".format(query.error))
                    return
                self.wfile.write("<html><body>Query Competed With No Result or Error</body></html>".format(query.result))
            else:
                self.wfile.write("<html><body>Query not complete.</body></html>\n")
            return

        else:
            self._set_headers()
            self.wfile.write("<html><body>Submit GET with query_text parameter to submit a query or query_id parameter"
                             "to retrieve results.</body></html>\n")

    def do_HEAD(self):
        self._set_headers()