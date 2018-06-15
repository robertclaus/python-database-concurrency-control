import os
import sys

from IsolationLevelSetter import IsolationLevelSetter
from DIBSEngine import DIBSEngine
from clients.MySQLClient import MySQLClient
from connectors.QueryGeneratorConnector import QueryGeneratorConnector
from connectors.QuerySets import TATP, TATP_Read

import config

config.MAX_SECONDS_TO_RUN = 120
config.MAX_QUERIES_TO_RUN = 50000

# Run #1 - Vary Write %
for query_set in [4]:
    QueryGeneratorConnector.possible_query_sets = TATP_Read.query_set
    for workers in [16]:
        config.NUMBER_OF_DATABASE_CLIENTS = workers
        for isolation_level in ['ru-phased','ru']:#,'ru']:#, 'ru-p']:
            dibs_policy = IsolationLevelSetter.run(isolation_level)
            QueryGeneratorConnector.last_isolation_level = isolation_level
            try:
                DIBSEngine.run(dibs_policy, MySQLClient, QueryGeneratorConnector)
            except IOError:
                sys.stdout.write("\n\nIO ERROR ENDED TEST\n\n")


'''
Running for 120 seconds or 25000 queries with 16 workers. In concurrency mode: <policies.PhasedPolicy.PhasedPolicy instance at 0x7f8c480f7e60> 
  Starting Connectors
Prepopulating Generator Queue
  Starting Isolation Engine
  Starting Database Clients
Changing Phases 1529007894.78
Readonly Start Admitting. 1529007894.78 
Readonly 1 Queries
Changing Phases 1529007895.03
Start Admitting 0 queries at 1529007895.03 on columns: call_forwarding.s_id,subscriber.sub_nbr,special_facility.s_id
Changing Phases 1529007895.03
Start Admitting 0 queries at 1529007895.03 on columns: special_facility.s_id,subscriber.s_id
Changing Phases 1529007895.03
Start Admitting 0 queries at 1529007895.03 on columns: call_forwarding.start_time,subscriber.sub_nbr
Changing Phases 1529007895.03
Start Admitting 0 queries at 1529007895.03 on columns: subscriber.sub_nbr
Changing Phases 1529007895.03
Readonly Start Admitting. 1529007895.03 
Readonly 0 Queries
Changing Phases 1529007895.03
Start Admitting 18 queries at 1529007895.03 on columns: call_forwarding.s_id,subscriber.sub_nbr,special_facility.s_id
Admitting 18 queries, with 0 remaining.
Changing Phases 1529007895.31
Start Admitting 26 queries at 1529007895.31 on columns: special_facility.s_id,subscriber.s_id
Admitting 26 queries, with 0 remaining.
Changing Phases 1529007895.51
Start Admitting 25 queries at 1529007895.51 on columns: call_forwarding.start_time,subscriber.sub_nbr
Admitting 25 queries, with 0 remaining.
Changing Phases 1529007895.66
Start Admitting 147 queries at 1529007895.66 on columns: subscriber.sub_nbr
Admitting 126 queries, with 21 remaining.
Changing Phases 1529007896.16
Readonly Start Admitting. 1529007896.16 
Readonly 4848 Queries
Changing Phases 1529007897.1
Start Admitting 110 queries at 1529007897.19 on columns: call_forwarding.s_id,subscriber.sub_nbr,special_facility.s_id
Admitting 110 queries, with 0 remaining.
Changing Phases 1529007897.77
Start Admitting 117 queries at 1529007897.78 on columns: special_facility.s_id,subscriber.s_id
Admitting 116 queries, with 1 remaining.
Changing Phases 1529007898.2
Start Admitting 117 queries at 1529007898.23 on columns: call_forwarding.start_time,subscriber.sub_nbr
Admitting 117 queries, with 0 remaining.
Changing Phases 1529007898.73
Start Admitting 877 queries at 1529007898.73 on columns: subscriber.sub_nbr
Admitting 126 queries, with 751 remaining.
Admitting 251 queries, with 625 remaining.
Admitting 376 queries, with 499 remaining.
Admitting 501 queries, with 373 remaining.
Admitting 626 queries, with 247 remaining.
Admitting 619 queries, with 121 remaining.
Admitting 360 queries, with 2 remaining.
Changing Phases 1529007901.62
Readonly Start Admitting. 1529007901.62 
Readonly 8035 Queries
Changing Phases 1529007904.58
Start Admitting 187 queries at 1529007904.72 on columns: call_forwarding.s_id,subscriber.sub_nbr,special_facility.s_id
Admitting 126 queries, with 61 remaining.
Changing Phases 1529007905.67
Start Admitting 179 queries at 1529007905.69 on columns: special_facility.s_id,subscriber.s_id
Admitting 126 queries, with 53 remaining.
Changing Phases 1529007906.39
Start Admitting 215 queries at 1529007906.44 on columns: call_forwarding.start_time,subscriber.sub_nbr
Admitting 126 queries, with 89 remaining.
Admitting 214 queries, with 0 remaining.
Changing Phases 1529007907.24
Start Admitting 1384 queries at 1529007907.24 on columns: subscriber.sub_nbr
Admitting 126 queries, with 1258 remaining.
Admitting 251 queries, with 1132 remaining.
Admitting 376 queries, with 1006 remaining.
Admitting 501 queries, with 880 remaining.
Admitting 626 queries, with 754 remaining.
Admitting 751 queries, with 628 remaining.
Admitting 876 queries, with 502 remaining.
Admitting 1001 queries, with 376 remaining.
Admitting 877 queries, with 250 remaining.
Admitting 625 queries, with 124 remaining.
Admitting 367 queries, with 4 remaining.
Changing Phases 1529007912.1
Readonly Start Admitting. 1529007912.11 
Readonly 7461 Queries
Stopping Engine
  Stopping Clients
  Connectors Clearing Completed Queries
  Stopping Connectors
Type [0] Count: 9682 [38%] Average Execution Time: 0.002056049552 [admit[11.740854] max[0.670534] +/- 2.165532]
Type [2] Count: 2697 [10%] Average Execution Time: 0.00429770519524 [admit[11.740533] max[0.671195] +/- 1.948329]
Type [4] Count: 9413 [37%] Average Execution Time: 0.0018771280331 [admit[11.740871] max[0.628437] +/- 2.035179]
Type [6] Count: 268 [1%] Average Execution Time: 0.0370549123679 [admit[0.053014] max[0.156006] +/- 0.687214]
Type [8] Count: 2381 [9%] Average Execution Time: 0.0504022616891 [admit[0.386455] max[0.132121] +/- 0.818427]
Type [10] Count: 254 [1%] Average Execution Time: 0.0508018497407 [admit[0.043798] max[0.221742] +/- 0.978492]
Type [12] Count: 357 [1%] Average Execution Time: 0.0189503314448 [admit[0.117662] max[0.096190] +/- 0.561651]
Average Worker Wait Time: 4.53712978065
Time spent processing completed queries 10.0417251587
Average Query Size From Generator: 243
Total Time: 18.8943390846
Completed: 25052
Utilization %: 75.9868299159
### ERROR: Utilization under 98% - Indicates this process was too slow.
Throughput (Q/s) : 1325.89977812

'''