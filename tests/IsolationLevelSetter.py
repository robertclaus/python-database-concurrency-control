import MySQLdb
import config
from DIBSEngine import DIBSEngine
from clients.MySQLClient import MySQLClient
from clients.PostgresClient import PostgresClient
from clients.SqliteClient import SqliteClient
from connectors.QueryGeneratorConnector import QueryGeneratorConnector
from connectors.QuerySets import Insert

from policies.NoIsolationPolicy import NoIsolationPolicy
from policies.PhasedPolicy import PhasedPolicy
from policies.ZeroConcurrencyPolicy import ZeroConcurrencyPolicy
from policies.DirectPredicatePolicy import DirectPredicatePolicy
from policies.NoIsolationPolicyWithParsing import  NoIsolationPolicyWithParsing
from policies.PhasedIntegratedPolicy import PhasedIntegratedPolicy


class IsolationLevelSetter:
    @staticmethod
    def run(isolation_level, dbClient):
            QueryGeneratorConnector.last_isolation_level = isolation_level

            policy = 0

            if isolation_level == 'ru':
              isolation_level = 0
            if isolation_level == 's':
              isolation_level = 1
            if isolation_level == 'rc':
              isolation_level = 2
            if isolation_level == 'rr':
              isolation_level = 3
            if isolation_level == 'ru-phased':
              isolation_level = 0
              policy = 1
            if isolation_level == 'ru-phased-integrated':
              isolation_level = 0
              policy = 5
            if isolation_level == 'ru-zerocc':
              isolation_level = 0
              policy = 2
            if isolation_level == 'ru-directcomparison':
              isolation_level = 0
              policy = 3
            if isolation_level == 'synthetic-setup':
              isolation_level = 4
            if isolation_level == 'ru-p':
              isolation_level = 0
              policy = 4

            query_text = [
            "SET GLOBAL TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;",
            "SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE;",
            "SET GLOBAL TRANSACTION ISOLATION LEVEL READ COMMITTED;",
            "SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ;",
            "DELETE FROM t.a;",
            ]

            dibs_policies = [
                NoIsolationPolicy(),
                PhasedPolicy(),
                ZeroConcurrencyPolicy(),
                DirectPredicatePolicy(),
                NoIsolationPolicyWithParsing(),
                PhasedIntegratedPolicy(),
            ]
            if (dbClient == MySQLClient) or isolation_level>3:
                client = dbClient()
                client.execute(query_text[isolation_level])

            if (dbClient == PostgresClient) and isolation_level <4:
                psql_query_text = [
                    "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;",
                    "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE;",
                    "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED;",
                    "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL LEVEL REPEATABLE READ;",
                ]
                PostgresClient.initialization_query = psql_query_text[isolation_level]


            return dibs_policies[policy]

    @staticmethod
    def setup(count, dbclient=MySQLClient):

        MAX_SECONDS_TO_RUN = config.MAX_SECONDS_TO_RUN
        MAX_QUERIES_TO_RUN = config.MAX_QUERIES_TO_RUN
        NUMBER_OF_DATABASE_CLIENTS = config.NUMBER_OF_DATABASE_CLIENTS
        oldQuerySets = QueryGeneratorConnector.possible_query_sets

        config.MAX_SECONDS_TO_RUN = 1000000
        config.MAX_QUERIES_TO_RUN = count
        config.NUMBER_OF_DATABASE_CLIENTS = 20

        QueryGeneratorConnector.possible_query_sets = Insert.query_set

        dibs_policy = IsolationLevelSetter.run("synthetic-setup", dbclient)

        DIBSEngine.run(dibs_policy, dbclient, QueryGeneratorConnector)

        config.MAX_SECONDS_TO_RUN = MAX_SECONDS_TO_RUN
        config.MAX_QUERIES_TO_RUN = MAX_QUERIES_TO_RUN
        config.NUMBER_OF_DATABASE_CLIENTS = NUMBER_OF_DATABASE_CLIENTS
        QueryGeneratorConnector.possible_query_sets = oldQuerySets