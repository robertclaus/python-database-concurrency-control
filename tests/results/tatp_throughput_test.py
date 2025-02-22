import os
import sys

from IsolationLevelSetter import IsolationLevelSetter
from DIBSEngine import DIBSEngine
from clients.MySQLClient import MySQLClient
from connectors.QueryGeneratorConnector import QueryGeneratorConnector
from connectors.QuerySets import TATP

import config

config.MAX_SECONDS_TO_RUN = 120
config.MAX_QUERIES_TO_RUN = 30000
config.MAX_QUERIES_IN_ENGINE = 15000
config.CLIENT_BUNDLE_SIZE = 3

config.DEFAULT_TARGET_DEPTH = 2000
config.GENERATOR_BUNDLE_SIZE = 10
config.DEFAULT_GENERATOR_WORKER_COUNT = 40
config.MAX_GENERATORS = 50
config.PREGENERATE_ALL_QUERIES = False
config.QUERIES_TO_NEXT_AT_TIME = 20

# Phased Policy
# Maximum queries to run in a round of phases. Note MAX_QUERIES_IN_ENGINE may restrict this anyways.
config.MAX_QUERIES_PER_PHASE = 10000
#15000 -> 7500 had little effect.

# Number of queries to try and admit at once before checking if we should move onto a different phase.
config.QUERIES_TO_ADMIT_AT_TIME = 500
config.QUERIES_TO_INITIALLY_ADMIT = 500

# If less than this many queries are in the phase, we will move on to the next phase (will always admit at least once before checking this)
config.MIN_QUERIES_TO_ADMIT = 60

for dbclient in [MySQLClient]:
    QueryGeneratorConnector.possible_query_sets = TATP.query_set
    for workers in [2, 4, 6, 8, 10, 12, 14, 16]:
        config.NUMBER_OF_DATABASE_CLIENTS = workers
        for isolation_level in ['ru-phased','ru', 'ru-directcomparison', 'ru-zerocc', 'ru', 's']:
            dibs_policy = IsolationLevelSetter.run(isolation_level,dbclient)

            try:
                DIBSEngine.run(dibs_policy, MySQLClient, QueryGeneratorConnector)
            except IOError:
                sys.stdout.write("\n\nIO ERROR ENDED TEST\n\n")