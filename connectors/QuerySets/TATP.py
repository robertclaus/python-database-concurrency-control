query_set = [
               "SELECT subscriber.s_id, subscriber.sub_nbr, \
               subscriber.bit_1, subscriber.bit_2, subscriber.bit_3, subscriber.bit_4, subscriber.bit_5, subscriber.bit_6, subscriber.bit_7, \
               subscriber.bit_8, subscriber.bit_9, subscriber.bit_10, \
               subscriber.hex_1, subscriber.hex_2, subscriber.hex_3, subscriber.hex_4, subscriber.hex_5, subscriber.hex_6, subscriber.hex_7, \
               subscriber.hex_8, subscriber.hex_9, subscriber.hex_10, \
               subscriber.byte2_1, subscriber.byte2_2, subscriber.byte2_3, subscriber.byte2_4, subscriber.byte2_5, \
               subscriber.byte2_6, subscriber.byte2_7, subscriber.byte2_8, subscriber.byte2_9, subscriber.byte2_10, \
               subscriber.msc_location, subscriber.vlr_location \
               FROM subscriber \
               WHERE subscriber.s_id = <non_uniform_rand_int_subscriber_size>; ",35, # was 35
               
               "SELECT call_forwarding.numberx \
               FROM special_facility, call_forwarding \
               WHERE \
               (special_facility.s_id = <non_uniform_rand_int_subscriber_size> \
               AND special_facility.sf_type = <rand_int_1_4> \
               AND special_facility.is_active = 1) \
               AND (call_forwarding.s_id = special_facility.s_id \
               AND call_forwarding.sf_type = special_facility.sf_type) \
               AND (call_forwarding.start_time <= <rand_0_8_16> \
               AND call_forwarding.end_time >= <rand_1_to_24>);",10, # was 10
               
               "SELECT access_info.data1, access_info.data2, access_info.data3, access_info.data4 \
               FROM access_info \
               WHERE access_info.s_id = <non_uniform_rand_int_subscriber_size> \
               AND access_info.ai_type = <rand_int_1_4>;",35, # was 35
               
               "UPDATE subscriber, special_facility \
               SET subscriber.bit_1 = <bit_rand>, special_facility.data_a = <rand_int_1_255> \
               WHERE subscriber.s_id = <non_uniform_rand_int_subscriber_size> \
               AND special_facility.s_id = <non_uniform_rand_int_subscriber_size> \
               AND special_facility.sf_type = <rand_int_1_4>;", 2, # Was 2
               
               "UPDATE subscriber \
               SET subscriber.vlr_location = <rand_int_1_big> \
               WHERE subscriber.sub_nbr = '<non_uniform_rand_int_subscriber_size_string>';",14, # Was 14
               
               "INSERT INTO call_forwarding (call_forwarding.s_id, call_forwarding.sf_type, call_forwarding.start_time, call_forwarding.end_time, call_forwarding.numberx) \
               SELECT subscriber.s_id, special_facility.sf_type ,<rand_0_8_16>,<rand_1_to_24>, <non_uniform_rand_int_subscriber_size> \
               FROM subscriber \
               LEFT OUTER JOIN special_facility ON subscriber.s_id = special_facility.s_id \
               WHERE subscriber.sub_nbr = '<non_uniform_rand_int_subscriber_size_string>' \
               ORDER BY RAND() LIMIT 1;",2, # Was 2, but self-conflicting was an issue.
               
               "DELETE call_forwarding FROM call_forwarding \
               INNER JOIN subscriber ON subscriber.s_id = call_forwarding.s_id \
               WHERE call_forwarding.sf_type = <rand_int_1_4> \
               AND call_forwarding.start_time = <rand_0_8_16> \
               AND subscriber.sub_nbr = '<non_uniform_rand_int_subscriber_size_string>';",2, # Was 2
               ]