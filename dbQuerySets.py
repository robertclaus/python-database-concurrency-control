query_sets = [
              [
               "SELECT a1 FROM a WHERE a2 = <randInt>;", 80,
               "UPDATE a SET a3 = <randInt> WHERE a2 = <randInt2>;", 20,
               ],
              [
               "SELECT a1 FROM a WHERE a2 = <randInt>;", 75,
               "UPDATE a SET a3 = <randInt> WHERE a2 = <randInt2>;", 25,
               ],
              [
               "SELECT a1 FROM a WHERE a2 = <randInt>;", 70,
               "UPDATE a SET a3 = <randInt> WHERE a2 = <randInt2>;", 30,
               ],
              [
               "SELECT a1 FROM a WHERE a2 = <randInt>;", 65,
               "UPDATE a SET a3 = <randInt> WHERE a2 = <randInt2>;", 35,
               ],
              [
               "SELECT a1 FROM a WHERE a2 = <randInt>;", 60,
               "UPDATE a SET a3 = <randInt> WHERE a2 = <randInt2>;", 40,
               ],
              [
               "SELECT a1 FROM a WHERE a2 = <randInt>;", 55,
               "UPDATE a SET a3 = <randInt> WHERE a2 = <randInt2>;", 45,
               ],
              [
               "SELECT a1 FROM a WHERE a2 = <randInt>;", 50,
               "UPDATE a SET a3 = <randInt> WHERE a2 = <randInt2>;", 50,
               ],
              [
               "SELECT a1 FROM a WHERE a2 = <randInt>;", 45,
               "UPDATE a SET a3 = <randInt> WHERE a2 = <randInt2>;", 55,
               ],
              [
               "SELECT a1 FROM a WHERE a2 = <randInt>;", 40,
               "UPDATE a SET a3 = <randInt> WHERE a2 = <randInt2>;", 60,
               ],
              [
               "SELECT a1 FROM a WHERE a2 = <randInt>;", 35,
               "UPDATE a SET a3 = <randInt> WHERE a2 = <randInt2>;", 65,
               ],
              [
               "SELECT a1 FROM a WHERE a2 = <randInt>;", 30,
               "UPDATE a SET a3 = <randInt> WHERE a2 = <randInt2>;", 70,
               ],
              [
               "SELECT a1 FROM a WHERE a2 = <randInt>;", 25,
               "UPDATE a SET a3 = <randInt> WHERE a2 = <randInt2>;", 75,
               ],
              [
               "SELECT a1 FROM a WHERE a2 = <randInt>;", 20,
               "UPDATE a SET a3 = <randInt> WHERE a2 = <randInt2>;", 80,
               ],
              [
               "SELECT a1 FROM a WHERE a2 = <randInt>;", 15,
               "UPDATE a SET a3 = <randInt> WHERE a2 = <randInt2>;", 85,
               ],
              [
               "SELECT a1 FROM a WHERE a2 = <randInt>;", 10,
               "UPDATE a SET a3 = <randInt> WHERE a2 = <randInt2>;", 90,
               ],
              [
               "SELECT a1 FROM a WHERE a2 = <randInt>;", 5,
               "UPDATE a SET a3 = <randInt> WHERE a2 = <randInt2>;", 95,
               ],
              [
               "SELECT a1 FROM a WHERE a2 = <randInt>;", 0,
               "UPDATE a SET a3 = <randInt> WHERE a2 = <randInt2>;", 100,
               ],
              [
               "UPDATE a SET a3=<randInt>;", 100,
               ],
              
              [
               "SELECT a3 FROM a WHERE a2=<randInt>;",100,
               ],
              
              [
               "UPDATE a SET a3=<randInt> WHERE a2=<randInt2>;",100,
               ],
              
              [
               "SELECT a2 FROM a WHERE a2=<randInt>;",100,
               ],
              
              [
               "INSERT INTO a (a1, a2, a3) VALUES (<randInt>, <randInt2>, <randInt3>);",100,
               ],
              
              [
               "SELECT a3 FROM a WHERE a1=<randInt>;",100,
               ],
              
              
              # TATP 20% Writes
              [
               "SELECT subscriber.s_id, subscriber.sub_nbr, \
               subscriber.bit_1, subscriber.bit_2, subscriber.bit_3, subscriber.bit_4, subscriber.bit_5, subscriber.bit_6, subscriber.bit_7, \
               subscriber.bit_8, subscriber.bit_9, subscriber.bit_10, \
               subscriber.hex_1, subscriber.hex_2, subscriber.hex_3, subscriber.hex_4, subscriber.hex_5, subscriber.hex_6, subscriber.hex_7, \
               subscriber.hex_8, subscriber.hex_9, subscriber.hex_10, \
               subscriber.byte2_1, subscriber.byte2_2, subscriber.byte2_3, subscriber.byte2_4, subscriber.byte2_5, \
               subscriber.byte2_6, subscriber.byte2_7, subscriber.byte2_8, subscriber.byte2_9, subscriber.byte2_10, \
               subscriber.msc_location, subscriber.vlr_location \
               FROM subscriber \
               WHERE subscriber.s_id = <non_uniform_rand_int_subscriber_size>; ",35,
               
               "SELECT call_forwarding.numberx \
               FROM special_facility, call_forwarding \
               WHERE \
               (special_facility.s_id = <non_uniform_rand_int_subscriber_size> \
                AND special_facility.sf_type = <rand_int_1_4> \
                AND special_facility.is_active = 1) \
               AND (call_forwarding.s_id = special_facility.s_id \
                    AND call_forwarding.sf_type = special_facility.sf_type) \
               AND (call_forwarding.start_time <= <rand_0_8_16> \
                    AND call_forwarding.end_time >= <rand_1_to_24>);",10,
               
               "SELECT access_info.data1, access_info.data2, access_info.data3, access_info.data4 \
               FROM access_info \
               WHERE access_info.s_id = <non_uniform_rand_int_subscriber_size> \
               AND access_info.ai_type = <rand_int_1_4>;",35,
               
               "UPDATE subscriber, special_facility \
               SET subscriber.bit_1 = <bit_rand>, special_facility.data_a = <rand_int_1_255> \
               WHERE subscriber.s_id = <non_uniform_rand_int_subscriber_size> \
               AND special_facility.s_id = <non_uniform_rand_int_subscriber_size> \
               AND special_facility.sf_type = <rand_int_1_4>;", 2,
               
               "UPDATE subscriber \
               SET subscriber.vlr_location = <rand_int_1_big> \
               WHERE subscriber.sub_nbr = '<non_uniform_rand_int_subscriber_size_string>';",14,
               
               "INSERT INTO call_forwarding (call_forwarding.s_id, call_forwarding.sf_type, call_forwarding.start_time, call_forwarding.end_time, call_forwarding.numberx) \
               SELECT subscriber.s_id, special_facility.sf_type ,<rand_0_8_16>,<rand_1_to_24>, <non_uniform_rand_int_subscriber_size> \
               FROM subscriber \
               LEFT OUTER JOIN special_facility ON subscriber.s_id = special_facility.s_id \
               WHERE subscriber.sub_nbr = '<non_uniform_rand_int_subscriber_size_string>' \
               ORDER BY RAND() LIMIT 1;",2,
               
               "DELETE call_forwarding FROM call_forwarding \
               INNER JOIN subscriber ON subscriber.s_id = call_forwarding.s_id \
               WHERE call_forwarding.sf_type = <rand_int_1_4> \
               AND call_forwarding.start_time = <rand_0_8_16> \
               AND subscriber.sub_nbr = '<non_uniform_rand_int_subscriber_size_string>';",2,
               ],
              
              
              # TATP 40% Writes
              [
               "SELECT subscriber.s_id, subscriber.sub_nbr, \
               subscriber.bit_1, subscriber.bit_2, subscriber.bit_3, subscriber.bit_4, subscriber.bit_5, subscriber.bit_6, subscriber.bit_7, \
               subscriber.bit_8, subscriber.bit_9, subscriber.bit_10, \
               subscriber.hex_1, subscriber.hex_2, subscriber.hex_3, subscriber.hex_4, subscriber.hex_5, subscriber.hex_6, subscriber.hex_7, \
               subscriber.hex_8, subscriber.hex_9, subscriber.hex_10, \
               subscriber.byte2_1, subscriber.byte2_2, subscriber.byte2_3, subscriber.byte2_4, subscriber.byte2_5, \
               subscriber.byte2_6, subscriber.byte2_7, subscriber.byte2_8, subscriber.byte2_9, subscriber.byte2_10, \
               subscriber.msc_location, subscriber.vlr_location \
               FROM subscriber \
               WHERE subscriber.s_id = <non_uniform_rand_int_subscriber_size>; ",26,
               
               "SELECT call_forwarding.numberx \
               FROM special_facility, call_forwarding \
               WHERE \
               (special_facility.s_id = <non_uniform_rand_int_subscriber_size> \
               AND special_facility.sf_type = <rand_int_1_4> \
               AND special_facility.is_active = 1) \
               AND (call_forwarding.s_id = special_facility.s_id \
               AND call_forwarding.sf_type = special_facility.sf_type) \
               AND (call_forwarding.start_time <= <rand_0_8_16> \
               AND call_forwarding.end_time >= <rand_1_to_24>);",8,
               
               "SELECT access_info.data1, access_info.data2, access_info.data3, access_info.data4 \
               FROM access_info \
               WHERE access_info.s_id = <non_uniform_rand_int_subscriber_size> \
               AND access_info.ai_type = <rand_int_1_4>;",26,
               
               "UPDATE subscriber, special_facility \
               SET subscriber.bit_1 = <bit_rand>, special_facility.data_a = <rand_int_1_255> \
               WHERE subscriber.s_id = <non_uniform_rand_int_subscriber_size> \
               AND special_facility.s_id = <non_uniform_rand_int_subscriber_size> \
               AND special_facility.sf_type = <rand_int_1_4>;", 4,
               
               "UPDATE subscriber \
               SET subscriber.vlr_location = <rand_int_1_big> \
               WHERE subscriber.sub_nbr = '<non_uniform_rand_int_subscriber_size_string>';",28,
               
               "INSERT INTO call_forwarding (call_forwarding.s_id, call_forwarding.sf_type, call_forwarding.start_time, call_forwarding.end_time, call_forwarding.numberx) \
               SELECT subscriber.s_id, special_facility.sf_type ,<rand_0_8_16>,<rand_1_to_24>, <non_uniform_rand_int_subscriber_size> \
               FROM subscriber \
               LEFT OUTER JOIN special_facility ON subscriber.s_id = special_facility.s_id \
               WHERE subscriber.sub_nbr = '<non_uniform_rand_int_subscriber_size_string>' \
               ORDER BY RAND() LIMIT 1;",4,
               
               "DELETE call_forwarding FROM call_forwarding \
               INNER JOIN subscriber ON subscriber.s_id = call_forwarding.s_id \
               WHERE call_forwarding.sf_type = <rand_int_1_4> \
               AND call_forwarding.start_time = <rand_0_8_16> \
               AND subscriber.sub_nbr = '<non_uniform_rand_int_subscriber_size_string>';",4,
               ],
              
              # TATP 60% Writes
              [
               "SELECT subscriber.s_id, subscriber.sub_nbr, \
               subscriber.bit_1, subscriber.bit_2, subscriber.bit_3, subscriber.bit_4, subscriber.bit_5, subscriber.bit_6, subscriber.bit_7, \
               subscriber.bit_8, subscriber.bit_9, subscriber.bit_10, \
               subscriber.hex_1, subscriber.hex_2, subscriber.hex_3, subscriber.hex_4, subscriber.hex_5, subscriber.hex_6, subscriber.hex_7, \
               subscriber.hex_8, subscriber.hex_9, subscriber.hex_10, \
               subscriber.byte2_1, subscriber.byte2_2, subscriber.byte2_3, subscriber.byte2_4, subscriber.byte2_5, \
               subscriber.byte2_6, subscriber.byte2_7, subscriber.byte2_8, subscriber.byte2_9, subscriber.byte2_10, \
               subscriber.msc_location, subscriber.vlr_location \
               FROM subscriber \
               WHERE subscriber.s_id = <non_uniform_rand_int_subscriber_size>; ",17.5,
               
               "SELECT call_forwarding.numberx \
               FROM special_facility, call_forwarding \
               WHERE \
               (special_facility.s_id = <non_uniform_rand_int_subscriber_size> \
               AND special_facility.sf_type = <rand_int_1_4> \
               AND special_facility.is_active = 1) \
               AND (call_forwarding.s_id = special_facility.s_id \
               AND call_forwarding.sf_type = special_facility.sf_type) \
               AND (call_forwarding.start_time <= <rand_0_8_16> \
               AND call_forwarding.end_time >= <rand_1_to_24>);",5,
               
               "SELECT access_info.data1, access_info.data2, access_info.data3, access_info.data4 \
               FROM access_info \
               WHERE access_info.s_id = <non_uniform_rand_int_subscriber_size> \
               AND access_info.ai_type = <rand_int_1_4>;",17.5,
               
               "UPDATE subscriber, special_facility \
               SET subscriber.bit_1 = <bit_rand>, special_facility.data_a = <rand_int_1_255> \
               WHERE subscriber.s_id = <non_uniform_rand_int_subscriber_size> \
               AND special_facility.s_id = <non_uniform_rand_int_subscriber_size> \
               AND special_facility.sf_type = <rand_int_1_4>;", 6,
               
               "UPDATE subscriber \
               SET subscriber.vlr_location = <rand_int_1_big> \
               WHERE subscriber.sub_nbr = '<non_uniform_rand_int_subscriber_size_string>';",42,
               
               "INSERT INTO call_forwarding (call_forwarding.s_id, call_forwarding.sf_type, call_forwarding.start_time, call_forwarding.end_time, call_forwarding.numberx) \
               SELECT subscriber.s_id, special_facility.sf_type ,<rand_0_8_16>,<rand_1_to_24>, <non_uniform_rand_int_subscriber_size> \
               FROM subscriber \
               LEFT OUTER JOIN special_facility ON subscriber.s_id = special_facility.s_id \
               WHERE subscriber.sub_nbr = '<non_uniform_rand_int_subscriber_size_string>' \
               ORDER BY RAND() LIMIT 1;",6,
               
               "DELETE call_forwarding FROM call_forwarding \
               INNER JOIN subscriber ON subscriber.s_id = call_forwarding.s_id \
               WHERE call_forwarding.sf_type = <rand_int_1_4> \
               AND call_forwarding.start_time = <rand_0_8_16> \
               AND subscriber.sub_nbr = '<non_uniform_rand_int_subscriber_size_string>';",6,
               ],
              
              # TATP 80% Writes
              [
               "SELECT subscriber.s_id, subscriber.sub_nbr, \
               subscriber.bit_1, subscriber.bit_2, subscriber.bit_3, subscriber.bit_4, subscriber.bit_5, subscriber.bit_6, subscriber.bit_7, \
               subscriber.bit_8, subscriber.bit_9, subscriber.bit_10, \
               subscriber.hex_1, subscriber.hex_2, subscriber.hex_3, subscriber.hex_4, subscriber.hex_5, subscriber.hex_6, subscriber.hex_7, \
               subscriber.hex_8, subscriber.hex_9, subscriber.hex_10, \
               subscriber.byte2_1, subscriber.byte2_2, subscriber.byte2_3, subscriber.byte2_4, subscriber.byte2_5, \
               subscriber.byte2_6, subscriber.byte2_7, subscriber.byte2_8, subscriber.byte2_9, subscriber.byte2_10, \
               subscriber.msc_location, subscriber.vlr_location \
               FROM subscriber \
               WHERE subscriber.s_id = <non_uniform_rand_int_subscriber_size>; ",9,
               
               "SELECT call_forwarding.numberx \
               FROM special_facility, call_forwarding \
               WHERE \
               (special_facility.s_id = <non_uniform_rand_int_subscriber_size> \
               AND special_facility.sf_type = <rand_int_1_4> \
               AND special_facility.is_active = 1) \
               AND (call_forwarding.s_id = special_facility.s_id \
               AND call_forwarding.sf_type = special_facility.sf_type) \
               AND (call_forwarding.start_time <= <rand_0_8_16> \
               AND call_forwarding.end_time >= <rand_1_to_24>);",2,
               
               "SELECT access_info.data1, access_info.data2, access_info.data3, access_info.data4 \
               FROM access_info \
               WHERE access_info.s_id = <non_uniform_rand_int_subscriber_size> \
               AND access_info.ai_type = <rand_int_1_4>;",9,
               
               "UPDATE subscriber, special_facility \
               SET subscriber.bit_1 = <bit_rand>, special_facility.data_a = <rand_int_1_255> \
               WHERE subscriber.s_id = <non_uniform_rand_int_subscriber_size> \
               AND special_facility.s_id = <non_uniform_rand_int_subscriber_size> \
               AND special_facility.sf_type = <rand_int_1_4>;", 8,
               
               "UPDATE subscriber \
               SET subscriber.vlr_location = <rand_int_1_big> \
               WHERE subscriber.sub_nbr = '<non_uniform_rand_int_subscriber_size_string>';",56,
               
               "INSERT INTO call_forwarding (call_forwarding.s_id, call_forwarding.sf_type, call_forwarding.start_time, call_forwarding.end_time, call_forwarding.numberx) \
               SELECT subscriber.s_id, special_facility.sf_type ,<rand_0_8_16>,<rand_1_to_24>, <non_uniform_rand_int_subscriber_size> \
               FROM subscriber \
               LEFT OUTER JOIN special_facility ON subscriber.s_id = special_facility.s_id \
               WHERE subscriber.sub_nbr = '<non_uniform_rand_int_subscriber_size_string>' \
               ORDER BY RAND() LIMIT 1;",8,
               
               "DELETE call_forwarding FROM call_forwarding \
               INNER JOIN subscriber ON subscriber.s_id = call_forwarding.s_id \
               WHERE call_forwarding.sf_type = <rand_int_1_4> \
               AND call_forwarding.start_time = <rand_0_8_16> \
               AND subscriber.sub_nbr = '<non_uniform_rand_int_subscriber_size_string>';",8,
               ],
              
              # TATP 100% Writes
              [
               "SELECT subscriber.s_id, subscriber.sub_nbr, \
               subscriber.bit_1, subscriber.bit_2, subscriber.bit_3, subscriber.bit_4, subscriber.bit_5, subscriber.bit_6, subscriber.bit_7, \
               subscriber.bit_8, subscriber.bit_9, subscriber.bit_10, \
               subscriber.hex_1, subscriber.hex_2, subscriber.hex_3, subscriber.hex_4, subscriber.hex_5, subscriber.hex_6, subscriber.hex_7, \
               subscriber.hex_8, subscriber.hex_9, subscriber.hex_10, \
               subscriber.byte2_1, subscriber.byte2_2, subscriber.byte2_3, subscriber.byte2_4, subscriber.byte2_5, \
               subscriber.byte2_6, subscriber.byte2_7, subscriber.byte2_8, subscriber.byte2_9, subscriber.byte2_10, \
               subscriber.msc_location, subscriber.vlr_location \
               FROM subscriber \
               WHERE subscriber.s_id = <non_uniform_rand_int_subscriber_size>; ",0,
               
               "SELECT call_forwarding.numberx \
               FROM special_facility, call_forwarding \
               WHERE \
               (special_facility.s_id = <non_uniform_rand_int_subscriber_size> \
               AND special_facility.sf_type = <rand_int_1_4> \
               AND special_facility.is_active = 1) \
               AND (call_forwarding.s_id = special_facility.s_id \
               AND call_forwarding.sf_type = special_facility.sf_type) \
               AND (call_forwarding.start_time <= <rand_0_8_16> \
               AND call_forwarding.end_time >= <rand_1_to_24>);",0,
               
               "SELECT access_info.data1, access_info.data2, access_info.data3, access_info.data4 \
               FROM access_info \
               WHERE access_info.s_id = <non_uniform_rand_int_subscriber_size> \
               AND access_info.ai_type = <rand_int_1_4>;",0,
               
               "UPDATE subscriber, special_facility \
               SET subscriber.bit_1 = <bit_rand>, special_facility.data_a = <rand_int_1_255> \
               WHERE subscriber.s_id = <non_uniform_rand_int_subscriber_size> \
               AND special_facility.s_id = <non_uniform_rand_int_subscriber_size> \
               AND special_facility.sf_type = <rand_int_1_4>;", 10,
               
               "UPDATE subscriber \
               SET subscriber.vlr_location = <rand_int_1_big> \
               WHERE subscriber.sub_nbr = '<non_uniform_rand_int_subscriber_size_string>';",70,
               
               "INSERT INTO call_forwarding (call_forwarding.s_id, call_forwarding.sf_type, call_forwarding.start_time, call_forwarding.end_time, call_forwarding.numberx) \
               SELECT subscriber.s_id, special_facility.sf_type ,<rand_0_8_16>,<rand_1_to_24>, <non_uniform_rand_int_subscriber_size> \
               FROM subscriber \
               LEFT OUTER JOIN special_facility ON subscriber.s_id = special_facility.s_id \
               WHERE subscriber.sub_nbr = '<non_uniform_rand_int_subscriber_size_string>' \
               ORDER BY RAND() LIMIT 1;",10,
               
               "DELETE call_forwarding FROM call_forwarding \
               INNER JOIN subscriber ON subscriber.s_id = call_forwarding.s_id \
               WHERE call_forwarding.sf_type = <rand_int_1_4> \
               AND call_forwarding.start_time = <rand_0_8_16> \
               AND subscriber.sub_nbr = '<non_uniform_rand_int_subscriber_size_string>';",10,
               ],
              
              
              # TATP Tailored To Predicate Locks
              [
               "SELECT \
              subscriber.bit_2, subscriber.bit_3, subscriber.bit_4, subscriber.bit_5, subscriber.bit_6, subscriber.bit_7, \
               subscriber.bit_8, subscriber.bit_9, subscriber.bit_10, \
               subscriber.hex_1, subscriber.hex_2, subscriber.hex_3, subscriber.hex_4, subscriber.hex_5, subscriber.hex_6, subscriber.hex_7, \
               subscriber.hex_8, subscriber.hex_9, subscriber.hex_10, \
               subscriber.byte2_1, subscriber.byte2_2, subscriber.byte2_3, subscriber.byte2_4, subscriber.byte2_5, \
               subscriber.byte2_6, subscriber.byte2_7, subscriber.byte2_8, subscriber.byte2_9, subscriber.byte2_10, \
               subscriber.msc_location \
               FROM subscriber \
               WHERE subscriber.s_id = <non_uniform_rand_int_subscriber_size>; ",35,
               
               "SELECT access_info.data1, access_info.data2, access_info.data3, access_info.data4 \
               FROM access_info \
               WHERE access_info.s_id = <non_uniform_rand_int_subscriber_size> \
               AND access_info.ai_type = <rand_int_1_4>;",35,
               
               "UPDATE subscriber, special_facility \
               SET subscriber.bit_1 = <bit_rand>, special_facility.data_a = <rand_int_1_255> \
               WHERE subscriber.s_id = <non_uniform_rand_int_subscriber_size> \
               AND special_facility.s_id = <non_uniform_rand_int_subscriber_size> \
               AND special_facility.sf_type = <rand_int_1_4>;", 10,
               
               "UPDATE subscriber \
               SET subscriber.vlr_location = <rand_int_1_big> \
               WHERE subscriber.sub_nbr = '<non_uniform_rand_int_subscriber_size_string>';",20,
               
               #"DELETE call_forwarding FROM call_forwarding \
               #INNER JOIN subscriber ON subscriber.s_id = call_forwarding.s_id \
               #WHERE call_forwarding.sf_type = <rand_int_1_4> \
               #AND call_forwarding.start_time = <rand_0_8_16> \
               #AND subscriber.sub_nbr = '<non_uniform_rand_int_subscriber_size_string>';",6,
               ],
              
              [ # Old version before collapsing into single queries
               "SELECT <non_uniform_rand_int_subscriber_size> \
               FROM Subscriber \
               WHERE sub_nbr = <non_uniform_rand_int_subscriber_size_string>; \
               \
               SELECT <sf_type bind sfid sf_type> \
               FROM Special_Facility \
               WHERE s_id = <s_id value subid>: \
               \
               VALUES (<s_id value subid>, <sf_type rnd sf_type>, \
               <rand_0_8_16>, <rand_1_to_24>, <non_uniform_rand_int_subscriber_size_string>); ",2,
               
               "UPDATE subscriber \
               SET bit_1 = <bit_rand> \
               WHERE s_id = <non_uniform_rand_int_subscriber_size>; \
               \
               UPDATE special_facility \
               SET data_a = <rand_int_1_255> \
               WHERE s_id = <non_uniform_rand_int_subscriber_size> \
               AND sf_type = <rand_int_1_4>; ",2,
               
               "DELETE FROM call_forwarding \
               WHERE call_forwarding.s_id IN ( \
               SELECT subscriber.s_id \
               FROM subscriber \
               WHERE subscriber.sub_nbr = '<non_uniform_rand_int_subscriber_size_string>' \
               ) \
               AND call_forwarding.sf_type = <rand_int_1_4> \
               AND call_forwarding.start_time = <rand_0_8_16>;",2,
               ]

              
              
]

