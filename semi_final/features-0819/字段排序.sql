-- 29个表（除去opposing_id）,对字段按照在训练集中出现次数进行排序
-- 29个表分别将join训练数据表和测试数据表，进行字段特征序列化
DROP TABLE IF EXISTS huo_client_ip_row;
CREATE TABLE IF NOT EXISTS huo_client_ip_row AS
SELECT client_ip, row_number() OVER(ORDER BY count_num) client_ip_row_number
FROM (
    SELECT client_ip, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY client_ip
) t;

DROP TABLE IF EXISTS huo_network_row;
CREATE TABLE IF NOT EXISTS huo_network_row AS
SELECT network, row_number() OVER(ORDER BY count_num) network_row_number
FROM (
    SELECT network, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY network
) t;

DROP TABLE IF EXISTS huo_device_sign_row;
CREATE TABLE IF NOT EXISTS huo_device_sign_row AS
SELECT device_sign, row_number() OVER(ORDER BY count_num) device_sign_row_number
FROM (
    SELECT device_sign, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY device_sign
) t;

DROP TABLE IF EXISTS huo_info_1_row;
CREATE TABLE IF NOT EXISTS huo_info_1_row AS
SELECT info_1, row_number() OVER(ORDER BY count_num) info_1_row_number
FROM (
    SELECT info_1, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY info_1
) t;

DROP TABLE IF EXISTS huo_info_2_row;
CREATE TABLE IF NOT EXISTS huo_info_2_row AS
SELECT info_2, row_number() OVER(ORDER BY count_num) info_2_row_number
FROM (
    SELECT info_2, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY info_2
) t;

DROP TABLE IF EXISTS huo_ip_prov_row;
CREATE TABLE IF NOT EXISTS huo_ip_prov_row AS
SELECT ip_prov, row_number() OVER(ORDER BY count_num) ip_prov_row_number
FROM (
    SELECT ip_prov, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY ip_prov
) t;

DROP TABLE IF EXISTS huo_ip_city_row;
CREATE TABLE IF NOT EXISTS huo_ip_city_row AS
SELECT ip_city, row_number() OVER(ORDER BY count_num) ip_city_row_number
FROM (
    SELECT ip_city, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY ip_city
) t;

DROP TABLE IF EXISTS huo_cert_prov_row;
CREATE TABLE IF NOT EXISTS huo_cert_prov_row AS
SELECT cert_prov, row_number() OVER(ORDER BY count_num) cert_prov_row_number
FROM (
    SELECT cert_prov, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY cert_prov
) t;

DROP TABLE IF EXISTS huo_cert_city_row;
CREATE TABLE IF NOT EXISTS huo_cert_city_row AS
SELECT cert_city, row_number() OVER(ORDER BY count_num) cert_city_row_number
FROM (
    SELECT cert_city, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY cert_city
) t;

DROP TABLE IF EXISTS huo_card_bin_prov_row;
CREATE TABLE IF NOT EXISTS huo_card_bin_prov_row AS
SELECT card_bin_prov, row_number() OVER(ORDER BY count_num) card_bin_prov_row_number
FROM (
    SELECT card_bin_prov, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY card_bin_prov
) t;

DROP TABLE IF EXISTS huo_card_bin_city_row;
CREATE TABLE IF NOT EXISTS huo_card_bin_city_row AS
SELECT card_bin_city, row_number() OVER(ORDER BY count_num) card_bin_city_row_number
FROM (
    SELECT card_bin_city, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY card_bin_city
) t;

DROP TABLE IF EXISTS huo_card_mobile_prov_row;
CREATE TABLE IF NOT EXISTS huo_card_mobile_prov_row AS
SELECT card_mobile_prov, row_number() OVER(ORDER BY count_num) card_mobile_prov_row_number
FROM (
    SELECT card_mobile_prov, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY card_mobile_prov
) t;

DROP TABLE IF EXISTS huo_card_mobile_city_row;
CREATE TABLE IF NOT EXISTS huo_card_mobile_city_row AS
SELECT card_mobile_city, row_number() OVER(ORDER BY count_num) card_mobile_city_row_number
FROM (
    SELECT card_mobile_city, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY card_mobile_city
) t;

DROP TABLE IF EXISTS huo_card_cert_prov_row;
CREATE TABLE IF NOT EXISTS huo_card_cert_prov_row AS
SELECT card_cert_prov, row_number() OVER(ORDER BY count_num) card_cert_prov_row_number
FROM (
    SELECT card_cert_prov, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY card_cert_prov
) t;

DROP TABLE IF EXISTS huo_card_cert_city_row;
CREATE TABLE IF NOT EXISTS huo_card_cert_city_row AS
SELECT card_cert_city, row_number() OVER(ORDER BY count_num) card_cert_city_row_number
FROM (
    SELECT card_cert_city, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY card_cert_city
) t;

DROP TABLE IF EXISTS huo_is_one_people_row;
CREATE TABLE IF NOT EXISTS huo_is_one_people_row AS
SELECT is_one_people, row_number() OVER(ORDER BY count_num) is_one_people_row_number
FROM (
    SELECT is_one_people, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY is_one_people
) t;

DROP TABLE IF EXISTS huo_mobile_oper_platform_row;
CREATE TABLE IF NOT EXISTS huo_mobile_oper_platform_row AS
SELECT mobile_oper_platform, row_number() OVER(ORDER BY count_num) mobile_oper_platform_row_number
FROM (
    SELECT mobile_oper_platform, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY mobile_oper_platform
) t;

DROP TABLE IF EXISTS huo_operation_channel_row;
CREATE TABLE IF NOT EXISTS huo_operation_channel_row AS
SELECT operation_channel, row_number() OVER(ORDER BY count_num) operation_channel_row_number
FROM (
    SELECT operation_channel, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY operation_channel
) t;

DROP TABLE IF EXISTS huo_pay_scene_row;
CREATE TABLE IF NOT EXISTS huo_pay_scene_row AS
SELECT pay_scene, row_number() OVER(ORDER BY count_num) pay_scene_row_number
FROM (
    SELECT pay_scene, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY pay_scene
) t;

DROP TABLE IF EXISTS huo_amt_row;
CREATE TABLE IF NOT EXISTS huo_amt_row AS
SELECT amt, row_number() OVER(ORDER BY count_num) amt_row_number
FROM (
    SELECT amt, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY amt
) t;

DROP TABLE IF EXISTS huo_card_cert_no_row;
CREATE TABLE IF NOT EXISTS huo_card_cert_no_row AS
SELECT card_cert_no, row_number() OVER(ORDER BY count_num) card_cert_no_row_number
FROM (
    SELECT card_cert_no, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY card_cert_no
) t;

DROP TABLE IF EXISTS huo_income_card_no_row;
CREATE TABLE IF NOT EXISTS huo_income_card_no_row AS
SELECT income_card_no, row_number() OVER(ORDER BY count_num) income_card_no_row_number
FROM (
    SELECT income_card_no, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY income_card_no
) t;

DROP TABLE IF EXISTS huo_income_card_cert_no_row;
CREATE TABLE IF NOT EXISTS huo_income_card_cert_no_row AS
SELECT income_card_cert_no, row_number() OVER(ORDER BY count_num) income_card_cert_no_row_number
FROM (
    SELECT income_card_cert_no, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY income_card_cert_no
) t;

DROP TABLE IF EXISTS huo_income_card_mobile_row;
CREATE TABLE IF NOT EXISTS huo_income_card_mobile_row AS
SELECT income_card_mobile, row_number() OVER(ORDER BY count_num) income_card_mobile_row_number
FROM (
    SELECT income_card_mobile, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY income_card_mobile
) t;

DROP TABLE IF EXISTS huo_income_card_bank_code_row;
CREATE TABLE IF NOT EXISTS huo_income_card_bank_code_row AS
SELECT income_card_bank_code, row_number() OVER(ORDER BY count_num) income_card_bank_code_row_number
FROM (
    SELECT income_card_bank_code, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY income_card_bank_code
) t;

DROP TABLE IF EXISTS huo_province_row;
CREATE TABLE IF NOT EXISTS huo_province_row AS
SELECT province, row_number() OVER(ORDER BY count_num) province_row_number
FROM (
    SELECT province, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY province
) t;

DROP TABLE IF EXISTS huo_city_row;
CREATE TABLE IF NOT EXISTS huo_city_row AS
SELECT city, row_number() OVER(ORDER BY count_num) city_row_number
FROM (
    SELECT city, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY city
) t;

DROP TABLE IF EXISTS huo_is_peer_pay_row;
CREATE TABLE IF NOT EXISTS huo_is_peer_pay_row AS
SELECT is_peer_pay, row_number() OVER(ORDER BY count_num) is_peer_pay_row_number
FROM (
    SELECT is_peer_pay, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY is_peer_pay
) t;

DROP TABLE IF EXISTS huo_version_row;
CREATE TABLE IF NOT EXISTS huo_version_row AS
SELECT version, row_number() OVER(ORDER BY count_num) version_row_number
FROM (
    SELECT version, count(1) AS count_num 
    FROM atec_1000w_ins_data 
    GROUP BY version
) t;
