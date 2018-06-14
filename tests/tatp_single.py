import os
import sys

from IsolationLevelSetter import IsolationLevelSetter
from DIBSEngine import DIBSEngine
from clients.MySQLClient import MySQLClient
from connectors.QueryGeneratorConnector import QueryGeneratorConnector
from connectors.QuerySets import query_sets

import config

config.MAX_SECONDS_TO_RUN = 120
config.MAX_QUERIES_TO_RUN = 50000

# Run #1 - Vary Write %
for query_set in [4]:
    QueryGeneratorConnector.possible_query_sets = query_sets[query_set]
    for workers in [16]:
        config.NUMBER_OF_DATABASE_CLIENTS = workers
        for isolation_level in ['ru', 'ru-p', 'ru-phased']:#,'ru-phased-integrated','ru', 'ru-p']:
            dibs_policy = IsolationLevelSetter.run(isolation_level)
            QueryGeneratorConnector.last_isolation_level = isolation_level
            try:
                DIBSEngine.run(dibs_policy, MySQLClient, QueryGeneratorConnector)
            except IOError:
                sys.stdout.write("\n\nIO ERROR ENDED TEST\n\n")


# Best So far
'''
Engine Stopped
Running for 120 seconds or 25000 queries with 16 workers. In concurrency mode: <policies.PhasedPolicy.PhasedPolicy instance at 0x7fbc29694638> 
  Starting Connectors
Prepopulating Generator Queue
  Starting Isolation Engine
Changing Phases 1529004027.51
Readonly Start Admitting. 1529004027.51 
  Starting Database Clients
Changing Phases 1529004028.61
Start Admitting 0 queries at 1529004028.61 on columns: call_forwarding.s_id,subscriber.sub_nbr,special_facility.s_id
Changing Phases 1529004028.61
Start Admitting 0 queries at 1529004028.61 on columns: special_facility.s_id,subscriber.s_id
Changing Phases 1529004028.61
Start Admitting 0 queries at 1529004028.61 on columns: call_forwarding.start_time,subscriber.sub_nbr
Changing Phases 1529004028.61
Start Admitting 0 queries at 1529004028.61 on columns: subscriber.sub_nbr
Changing Phases 1529004028.61
Readonly Start Admitting. 1529004028.61 
Changing Phases 1529004028.61
Start Admitting 71 queries at 1529004028.64 on columns: call_forwarding.s_id,subscriber.sub_nbr,special_facility.s_id
Admitting 71 queries, with 0 remaining.
Changing Phases 1529004028.92
Start Admitting 83 queries at 1529004028.93 on columns: special_facility.s_id,subscriber.s_id
Admitting 83 queries, with 0 remaining.
Changing Phases 1529004029.81
Start Admitting 88 queries at 1529004029.83 on columns: call_forwarding.start_time,subscriber.sub_nbr
Admitting 88 queries, with 0 remaining.
Changing Phases 1529004030.28
Start Admitting 542 queries at 1529004030.28 on columns: subscriber.sub_nbr
Admitting 126 queries, with 416 remaining.
Admitting 251 queries, with 290 remaining.
Admitting 376 queries, with 164 remaining.
Admitting 453 queries, with 38 remaining.
Changing Phases 1529004032.46
Readonly Start Admitting. 1529004032.46 
Changing Phases 1529004038.46
Start Admitting 438 queries at 1529004039.16 on columns: call_forwarding.s_id,subscriber.sub_nbr,special_facility.s_id
Admitting 126 queries, with 312 remaining.
Admitting 251 queries, with 186 remaining.
Admitting 376 queries, with 60 remaining.
Changing Phases 1529004041.48
Start Admitting 397 queries at 1529004041.52 on columns: special_facility.s_id,subscriber.s_id
Admitting 126 queries, with 271 remaining.
Admitting 251 queries, with 145 remaining.
Admitting 376 queries, with 19 remaining.
Changing Phases 1529004042.84
Start Admitting 410 queries at 1529004043.09 on columns: call_forwarding.start_time,subscriber.sub_nbr
Admitting 126 queries, with 284 remaining.
Admitting 251 queries, with 158 remaining.
Admitting 376 queries, with 32 remaining.
Changing Phases 1529004044.56
Start Admitting 2755 queries at 1529004044.56 on columns: subscriber.sub_nbr
Admitting 126 queries, with 2629 remaining.
Admitting 251 queries, with 2503 remaining.
Admitting 376 queries, with 2377 remaining.
Admitting 501 queries, with 2251 remaining.
Admitting 626 queries, with 2125 remaining.
Admitting 751 queries, with 1999 remaining.
Admitting 876 queries, with 1873 remaining.
Admitting 1001 queries, with 1747 remaining.
Admitting 1126 queries, with 1621 remaining.
Admitting 1251 queries, with 1495 remaining.
Admitting 1376 queries, with 1369 remaining.
Admitting 1501 queries, with 1243 remaining.
Admitting 1626 queries, with 1117 remaining.
Admitting 1751 queries, with 991 remaining.
Admitting 1876 queries, with 865 remaining.
Admitting 1855 queries, with 739 remaining.
Admitting 1603 queries, with 613 remaining.
Admitting 1351 queries, with 487 remaining.
Admitting 1099 queries, with 361 remaining.
Admitting 847 queries, with 235 remaining.
Admitting 595 queries, with 109 remaining.
Admitting 321 queries, with 5 remaining.
Changing Phases 1529004055.21
Readonly Start Admitting. 1529004055.21 
Stopping Engine
  Stopping Clients
  Connectors Clearing Completed Queries
Changing Phases 1529004061.82
Start Admitting 675 queries at 1529004063.12 on columns: call_forwarding.s_id,subscriber.sub_nbr,special_facility.s_id
Admitting 126 queries, with 549 remaining.
  Stopping Connectors
Type [0] Count: 19673 Average Execution Time: 0.00155923406903 [admit[14.808573] max[0.603362] +/- 2.434890]
Type [2] Count: 5526 Average Execution Time: 0.00385941019793 [admit[14.805149] max[0.829864] +/- 2.912560]
Type [4] Count: 18925 Average Execution Time: 0.00164648489404 [admit[14.808576] max[0.605745] +/- 2.674662]
Type [6] Count: 461 Average Execution Time: 0.0526417936006 [admit[0.145875] max[0.431593] +/- 1.634886]
Type [8] Count: 3254 Average Execution Time: 0.0588577754965 [admit[0.565738] max[0.370234] +/- 2.558165]
Type [10] Count: 449 Average Execution Time: 0.0360680454823 [admit[0.187167] max[0.131255] +/- 0.707410]
Type [12] Count: 466 Average Execution Time: 0.0258036309557 [admit[0.156733] max[0.232687] +/- 1.096114]
Average Worker Wait Time: 6.61402207613
Time spent processing completed queries 27.5004854202
Average Query Size From Generator: 245
Total Time: 34.3118400574
Completed: 48754
Utilization %: 80.7237907816
### ERROR: Utilization under 98% - Indicates this process was too slow.
Throughput (Q/s) : 1420.90893168
'''