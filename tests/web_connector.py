
from QueryFlowTester import QueryFlowTester
from clients.MySQLClient import MySQLClient
from connectors.WebConnector import WebConnector
from policies.ZeroConcurrencyPolicy import ZeroConcurrencyPolicy

time_to_run = 1000000
max_queries = 2000

policy = ZeroConcurrencyPolicy()

QueryFlowTester.run(policy, MySQLClient, WebConnector, 10000, 5, 1000)