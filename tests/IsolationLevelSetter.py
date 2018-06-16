import MySQLdb
import config

from policies.NoIsolationPolicy import NoIsolationPolicy
from policies.PhasedPolicy import PhasedPolicy
from policies.ZeroConcurrencyPolicy import ZeroConcurrencyPolicy
from policies.DirectPredicatePolicy import DirectPredicatePolicy
from policies.NoIsolationPolicyWithParsing import  NoIsolationPolicyWithParsing
from policies.PhasedIntegratedPolicy import PhasedIntegratedPolicy


class IsolationLevelSetter:
    @staticmethod
    def run(isolation_level):

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

            conn = MySQLdb.connect(host=config.MYSQL_HOST,user=config.MYSQL_USER,passwd=config.MYSQL_PASSWORD,db=config.MYSQL_DB_NAME)
            cur = conn.cursor()
            cur.execute(query_text[isolation_level])
            conn.commit()

            return dibs_policies[policy]