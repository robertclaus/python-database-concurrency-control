import os
import sys

from policies.PhasedPolicy import PhasedPolicy
from tests.IsolationLevelSetter import IsolationLevelSetter
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
    PhasedPolicy.lock_combinations = []
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
Type [0] Count: 20198 [40%] Average Execution Time: 0.000477020333741 [admit[0.000458] max[0.013989] +/- 0.045335]
Type [2] Count: 9968 [19%] Average Execution Time: 0.000425476204143 [admit[0.000631] max[0.014723] +/- 0.033117]
Type [4] Count: 19834 [39%] Average Execution Time: 0.000328831390422 [admit[0.002111] max[0.023430] +/- 0.049727]
Average Worker Wait Time: 16.8652654439
Time spent processing completed queries 6.74439072609
Average Query Size From Generator: 54
Total Generator Count: 30
Total Time: 20.6673080921
Completed: 50000
Utilization %: 18.3964095917
### ERROR: Utilization under 98% - Indicates this process was too slow.
Throughput (Q/s) : 2419.27975221

 csv,20.6673080921,16,50000,2419.27975221,18.3964095917,477.020333741,425.476204143,328.831390422,20198,9968,19834,0,477.020333741,20198,2,425.476204143,9968,4,328.831390422,19834, ru-phased, 16, ['SELECT subscriber.s_id, subscriber.sub_nbr,                subscriber.bit_1, subscriber.bit_2, subscriber.bit_3, subscriber.bit_4, subscriber.bit_5, subscriber.bit_6, subscriber.bit_7,                subscriber.bit_8, subscriber.bit_9, subscriber.bit_10,                subscriber.hex_1, subscriber.hex_2, subscriber.hex_3, subscriber.hex_4, subscriber.hex_5, subscriber.hex_6, subscriber.hex_7,                subscriber.hex_8, subscriber.hex_9, subscriber.hex_10,                subscriber.byte2_1, subscriber.byte2_2, subscriber.byte2_3, subscriber.byte2_4, subscriber.byte2_5,                subscriber.byte2_6, subscriber.byte2_7, subscriber.byte2_8, subscriber.byte2_9, subscriber.byte2_10,                subscriber.msc_location, subscriber.vlr_location                FROM subscriber                WHERE subscriber.s_id = <non_uniform_rand_int_subscriber_size>; ', 40, 'SELECT call_forwarding.numberx                FROM special_facility, call_forwarding                WHERE                (special_facility.s_id = <non_uniform_rand_int_subscriber_size>                AND special_facility.sf_type = <rand_int_1_4>                AND special_facility.is_active = 1)                AND (call_forwarding.s_id = special_facility.s_id                AND call_forwarding.sf_type = special_facility.sf_type)                AND (call_forwarding.start_time <= <rand_0_8_16>                AND call_forwarding.end_time >= <rand_1_to_24>);', 20, 'SELECT access_info.data1, access_info.data2, access_info.data3, access_info.data4                FROM access_info                WHERE access_info.s_id = <non_uniform_rand_int_subscriber_size>                AND access_info.ai_type = <rand_int_1_4>;', 40] 

VS

Type [0] Count: 20463 [40%] Average Execution Time: 0.000632528090578 [admit[0.000065] max[0.008233] +/- 0.020068]
Type [2] Count: 9725 [19%] Average Execution Time: 0.00058541319977 [admit[0.000058] max[0.007532] +/- 0.012587]
Type [4] Count: 19812 [39%] Average Execution Time: 0.000445403945391 [admit[0.000139] max[0.009653] +/- 0.017007]
Average Worker Wait Time: 18.3838922977
Time spent processing completed queries 9.61602568626
Average Query Size From Generator: 53
Total Generator Count: 13
Total Time: 24.9414401054
Completed: 50000
Utilization %: 26.2917769783
### ERROR: Utilization under 98% - Indicates this process was too slow.
Throughput (Q/s) : 2004.69579097

 csv,24.9414401054,16,50000,2004.69579097,26.2917769783,632.528090578,585.41319977,445.403945391,20463,9725,19812,0,632.528090578,20463,2,585.41319977,9725,4,445.403945391,19812, ru, 16, ['SELECT subscriber.s_id, subscriber.sub_nbr,                subscriber.bit_1, subscriber.bit_2, subscriber.bit_3, subscriber.bit_4, subscriber.bit_5, subscriber.bit_6, subscriber.bit_7,                subscriber.bit_8, subscriber.bit_9, subscriber.bit_10,                subscriber.hex_1, subscriber.hex_2, subscriber.hex_3, subscriber.hex_4, subscriber.hex_5, subscriber.hex_6, subscriber.hex_7,                subscriber.hex_8, subscriber.hex_9, subscriber.hex_10,                subscriber.byte2_1, subscriber.byte2_2, subscriber.byte2_3, subscriber.byte2_4, subscriber.byte2_5,                subscriber.byte2_6, subscriber.byte2_7, subscriber.byte2_8, subscriber.byte2_9, subscriber.byte2_10,                subscriber.msc_location, subscriber.vlr_location                FROM subscriber                WHERE subscriber.s_id = <non_uniform_rand_int_subscriber_size>; ', 40, 'SELECT call_forwarding.numberx                FROM special_facility, call_forwarding                WHERE                (special_facility.s_id = <non_uniform_rand_int_subscriber_size>                AND special_facility.sf_type = <rand_int_1_4>                AND special_facility.is_active = 1)                AND (call_forwarding.s_id = special_facility.s_id                AND call_forwarding.sf_type = special_facility.sf_type)                AND (call_forwarding.start_time <= <rand_0_8_16>                AND call_forwarding.end_time >= <rand_1_to_24>);', 20, 'SELECT access_info.data1, access_info.data2, access_info.data3, access_info.data4                FROM access_info                WHERE access_info.s_id = <non_uniform_rand_int_subscriber_size>                AND access_info.ai_type = <rand_int_1_4>;', 40] 
'''