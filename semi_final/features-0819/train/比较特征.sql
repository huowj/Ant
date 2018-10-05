-- 2个特征表：
-- (60) wen_train_compare_last_hour
-- (60) wen_train_compare_last_day

DROP TABLE IF EXISTS temp;
CREATE TABLE IF NOT EXISTS temp AS
SELECT a.*, dense_rank() OVER(PARTITION BY user_id ORDER BY gmt_occur) hour_order 
FROM atec_1000w_ins_data a;

DROP TABLE IF EXISTS temp_compare_last_hour;
CREATE TABLE IF NOT EXISTS temp_compare_last_hour AS
SELECT t1.event_id AS event_id, IF(t1.client_ip = t2.client_ip, 1, 0) AS client_ip_last_hour_eq,
    IF(t1.network = t2.network, 1, 0) AS network_last_hour_eq,
    IF(t1.device_sign = t2.device_sign, 1, 0) AS device_sign_last_hour_eq,
    IF(t1.info_1 = t2.info_1, 1, 0) AS info_1_last_hour_eq,
    IF(t1.info_2 = t2.info_2, 1, 0) AS info_2_last_hour_eq,
    IF(t1.ip_prov = t2.ip_prov, 1, 0) AS ip_prov_last_hour_eq,
    IF(t1.ip_city = t2.ip_city, 1, 0) AS ip_city_last_hour_eq,
    IF(t1.cert_prov = t2.cert_prov, 1, 0) AS cert_prov_last_hour_eq,
    IF(t1.cert_city = t2.cert_city, 1, 0) AS cert_city_last_hour_eq,
    IF(t1.card_bin_prov = t2.card_bin_prov, 1, 0) AS card_bin_prov_last_hour_eq,
    IF(t1.card_bin_city = t2.card_bin_city, 1, 0) AS card_bin_city_last_hour_eq,
    IF(t1.card_mobile_prov = t2.card_mobile_prov, 1, 0) AS card_mobile_prov_last_hour_eq,
    IF(t1.card_mobile_city = t2.card_mobile_city, 1, 0) AS card_mobile_city_last_hour_eq,
    IF(t1.card_cert_prov = t2.card_cert_prov, 1, 0) AS card_cert_prov_last_hour_eq,
    IF(t1.card_cert_city = t2.card_cert_city, 1, 0) AS card_cert_city_last_hour_eq,
    IF(t1.is_one_people = t2.is_one_people, 1, 0) AS is_one_people_last_hour_eq,
    IF(t1.mobile_oper_platform = t2.mobile_oper_platform, 1, 0) AS mobile_oper_platform_last_hour_eq,
    IF(t1.operation_channel = t2.operation_channel, 1, 0) AS operation_channel_last_hour_eq,
    IF(t1.pay_scene = t2.pay_scene, 1, 0) AS pay_scene_last_hour_eq,
    IF(t1.amt = t2.amt, 1, 0) AS amt_last_hour_eq,
    IF(t1.card_cert_no = t2.card_cert_no, 1, 0) AS card_cert_no_last_hour_eq,
    IF(t1.opposing_id = t2.opposing_id, 1, 0) AS opposing_id_last_hour_eq,
    IF(t1.income_card_no = t2.income_card_no, 1, 0) AS income_card_no_last_hour_eq,
    IF(t1.income_card_cert_no = t2.income_card_cert_no, 1, 0) AS income_card_cert_no_last_hour_eq,
    IF(t1.income_card_mobile = t2.income_card_mobile, 1, 0) AS income_card_mobile_last_hour_eq,
    IF(t1.income_card_bank_code = t2.income_card_bank_code, 1, 0) AS income_card_bank_code_last_hour_eq,
    IF(t1.province = t2.province, 1, 0) AS province_last_hour_eq,
    IF(t1.city = t2.city, 1, 0) AS city_last_hour_eq,
    IF(t1.is_peer_pay = t2.is_peer_pay, 1, 0) AS is_peer_pay_last_hour_eq,
    IF(t1.version = t2.version, 1, 0) AS version_last_hour_eq
FROM temp t1 LEFT JOIN temp t2
ON t1.user_id = t2.user_id and t1.hour_order = t2.hour_order + 1;

-- 60个特征(和上个小时进行比较)
DROP TABLE IF EXISTS wen_train_compare_last_hour;
CREATE TABLE IF NOT EXISTS wen_train_compare_last_hour AS
SELECT event_id,
    AVG(client_ip_last_hour_eq) AS client_ip_last_hour_eq_avg,
    AVG(network_last_hour_eq) AS network_last_hour_eq_avg,
    AVG(device_sign_last_hour_eq) AS device_sign_last_hour_eq_avg,
    AVG(info_1_last_hour_eq) AS info_1_last_hour_eq_avg,
    AVG(info_2_last_hour_eq) AS info_2_last_hour_eq_avg,
    AVG(ip_prov_last_hour_eq) AS ip_prov_last_hour_eq_avg,
    AVG(ip_city_last_hour_eq) AS ip_city_last_hour_eq_avg,
    AVG(cert_prov_last_hour_eq) AS cert_prov_last_hour_eq_avg,
    AVG(cert_city_last_hour_eq) AS cert_city_last_hour_eq_avg,
    AVG(card_bin_prov_last_hour_eq) AS card_bin_prov_last_hour_eq_avg,
    AVG(card_bin_city_last_hour_eq) AS card_bin_city_last_hour_eq_avg,
    AVG(card_mobile_prov_last_hour_eq) AS card_mobile_prov_last_hour_eq_avg,
    AVG(card_mobile_city_last_hour_eq) AS card_mobile_city_last_hour_eq_avg,
    AVG(card_cert_prov_last_hour_eq) AS card_cert_prov_last_hour_eq_avg,
    AVG(card_cert_city_last_hour_eq) AS card_cert_city_last_hour_eq_avg,
    AVG(is_one_people_last_hour_eq) AS is_one_people_last_hour_eq_avg,
    AVG(mobile_oper_platform_last_hour_eq) AS mobile_oper_platform_last_hour_eq_avg,
    AVG(operation_channel_last_hour_eq) AS operation_channel_last_hour_eq_avg,
    AVG(pay_scene_last_hour_eq) AS pay_scene_last_hour_eq_avg,
    AVG(amt_last_hour_eq) AS amt_last_hour_eq_avg,
    AVG(card_cert_no_last_hour_eq) AS card_cert_no_last_hour_eq_avg,
    AVG(opposing_id_last_hour_eq) AS opposing_id_last_hour_eq_avg,
    AVG(income_card_no_last_hour_eq) AS income_card_no_last_hour_eq_avg,
    AVG(income_card_cert_no_last_hour_eq) AS income_card_cert_no_last_hour_eq_avg,
    AVG(income_card_mobile_last_hour_eq) AS income_card_mobile_last_hour_eq_avg,
    AVG(income_card_bank_code_last_hour_eq) AS income_card_bank_code_last_hour_eq_avg,
    AVG(province_last_hour_eq) AS province_last_hour_eq_avg,
    AVG(city_last_hour_eq) AS city_last_hour_eq_avg,
    AVG(is_peer_pay_last_hour_eq) AS is_peer_pay_last_hour_eq_avg,
    AVG(version_last_hour_eq) AS version_last_hour_eq_avg,
    SUM(client_ip_last_hour_eq) AS client_ip_last_hour_eq_sum,
    SUM(network_last_hour_eq) AS network_last_hour_eq_sum,
    SUM(device_sign_last_hour_eq) AS device_sign_last_hour_eq_sum,
    SUM(info_1_last_hour_eq) AS info_1_last_hour_eq_sum,
    SUM(info_2_last_hour_eq) AS info_2_last_hour_eq_sum,
    SUM(ip_prov_last_hour_eq) AS ip_prov_last_hour_eq_sum,
    SUM(ip_city_last_hour_eq) AS ip_city_last_hour_eq_sum,
    SUM(cert_prov_last_hour_eq) AS cert_prov_last_hour_eq_sum,
    SUM(cert_city_last_hour_eq) AS cert_city_last_hour_eq_sum,
    SUM(card_bin_prov_last_hour_eq) AS card_bin_prov_last_hour_eq_sum,
    SUM(card_bin_city_last_hour_eq) AS card_bin_city_last_hour_eq_sum,
    SUM(card_mobile_prov_last_hour_eq) AS card_mobile_prov_last_hour_eq_sum,
    SUM(card_mobile_city_last_hour_eq) AS card_mobile_city_last_hour_eq_sum,
    SUM(card_cert_prov_last_hour_eq) AS card_cert_prov_last_hour_eq_sum,
    SUM(card_cert_city_last_hour_eq) AS card_cert_city_last_hour_eq_sum,
    SUM(is_one_people_last_hour_eq) AS is_one_people_last_hour_eq_sum,
    SUM(mobile_oper_platform_last_hour_eq) AS mobile_oper_platform_last_hour_eq_sum,
    SUM(operation_channel_last_hour_eq) AS operation_channel_last_hour_eq_sum,
    SUM(pay_scene_last_hour_eq) AS pay_scene_last_hour_eq_sum,
    SUM(amt_last_hour_eq) AS amt_last_hour_eq_sum,
    SUM(card_cert_no_last_hour_eq) AS card_cert_no_last_hour_eq_sum,
    SUM(opposing_id_last_hour_eq) AS opposing_id_last_hour_eq_sum,
    SUM(income_card_no_last_hour_eq) AS income_card_no_last_hour_eq_sum,
    SUM(income_card_cert_no_last_hour_eq) AS income_card_cert_no_last_hour_eq_sum,
    SUM(income_card_mobile_last_hour_eq) AS income_card_mobile_last_hour_eq_sum,
    SUM(income_card_bank_code_last_hour_eq) AS income_card_bank_code_last_hour_eq_sum,
    SUM(province_last_hour_eq) AS province_last_hour_eq_sum,
    SUM(city_last_hour_eq) AS city_last_hour_eq_sum,
    SUM(is_peer_pay_last_hour_eq) AS is_peer_pay_last_hour_eq_sum,
    SUM(version_last_hour_eq) AS version_last_hour_eq_sum
FROM temp_compare_last_hour GROUP BY event_id;

-- 清除临时表
DROP TABLE IF EXISTS temp;
DROP TABLE IF EXISTS temp_compare_last_hour;


----------------------------------------------------------------------------------------------------------------


-- 计算gmt_days
DROP TABLE IF EXISTS temp_gmt_days;
CREATE TABLE IF NOT EXISTS temp_gmt_days AS
SELECT a.*, datediff(concat(gmt_occur, ':00:00'), '2017-09-05 00:00:00', 'dd') as gmt_days
FROM atec_1000w_ins_data a;

-- 利用gmt_days, 构造day_order
DROP TABLE IF EXISTS temp_day_order;
CREATE TABLE IF NOT EXISTS temp_day_order AS
SELECT t.*, dense_rank() OVER (PARTITION BY user_id ORDER BY gmt_days) day_order 
FROM temp_gmt_days t;

DROP TABLE IF EXISTS temp_compare_last_day;
CREATE TABLE IF NOT EXISTS temp_compare_last_day AS
SELECT t1.event_id AS event_id, IF(t1.client_ip = t2.client_ip, 1, 0) AS client_ip_last_day_eq,
    IF(t1.network = t2.network, 1, 0) AS network_last_day_eq,
    IF(t1.device_sign = t2.device_sign, 1, 0) AS device_sign_last_day_eq,
    IF(t1.info_1 = t2.info_1, 1, 0) AS info_1_last_day_eq,
    IF(t1.info_2 = t2.info_2, 1, 0) AS info_2_last_day_eq,
    IF(t1.ip_prov = t2.ip_prov, 1, 0) AS ip_prov_last_day_eq,
    IF(t1.ip_city = t2.ip_city, 1, 0) AS ip_city_last_day_eq,
    IF(t1.cert_prov = t2.cert_prov, 1, 0) AS cert_prov_last_day_eq,
    IF(t1.cert_city = t2.cert_city, 1, 0) AS cert_city_last_day_eq,
    IF(t1.card_bin_prov = t2.card_bin_prov, 1, 0) AS card_bin_prov_last_day_eq,
    IF(t1.card_bin_city = t2.card_bin_city, 1, 0) AS card_bin_city_last_day_eq,
    IF(t1.card_mobile_prov = t2.card_mobile_prov, 1, 0) AS card_mobile_prov_last_day_eq,
    IF(t1.card_mobile_city = t2.card_mobile_city, 1, 0) AS card_mobile_city_last_day_eq,
    IF(t1.card_cert_prov = t2.card_cert_prov, 1, 0) AS card_cert_prov_last_day_eq,
    IF(t1.card_cert_city = t2.card_cert_city, 1, 0) AS card_cert_city_last_day_eq,
    IF(t1.is_one_people = t2.is_one_people, 1, 0) AS is_one_people_last_day_eq,
    IF(t1.mobile_oper_platform = t2.mobile_oper_platform, 1, 0) AS mobile_oper_platform_last_day_eq,
    IF(t1.operation_channel = t2.operation_channel, 1, 0) AS operation_channel_last_day_eq,
    IF(t1.pay_scene = t2.pay_scene, 1, 0) AS pay_scene_last_day_eq,
    IF(t1.amt = t2.amt, 1, 0) AS amt_last_day_eq,
    IF(t1.card_cert_no = t2.card_cert_no, 1, 0) AS card_cert_no_last_day_eq,
    IF(t1.opposing_id = t2.opposing_id, 1, 0) AS opposing_id_last_day_eq,
    IF(t1.income_card_no = t2.income_card_no, 1, 0) AS income_card_no_last_day_eq,
    IF(t1.income_card_cert_no = t2.income_card_cert_no, 1, 0) AS income_card_cert_no_last_day_eq,
    IF(t1.income_card_mobile = t2.income_card_mobile, 1, 0) AS income_card_mobile_last_day_eq,
    IF(t1.income_card_bank_code = t2.income_card_bank_code, 1, 0) AS income_card_bank_code_last_day_eq,
    IF(t1.province = t2.province, 1, 0) AS province_last_day_eq,
    IF(t1.city = t2.city, 1, 0) AS city_last_day_eq,
    IF(t1.is_peer_pay = t2.is_peer_pay, 1, 0) AS is_peer_pay_last_day_eq,
    IF(t1.version = t2.version, 1, 0) AS version_last_day_eq
FROM temp_day_order t1 LEFT JOIN temp_day_order t2
ON t1.user_id = t2.user_id and t1.day_order = t2.day_order + 1;

-- 60个特征(和前一次消费天进行比较)
DROP TABLE IF EXISTS wen_train_compare_last_day;
CREATE TABLE IF NOT EXISTS wen_train_compare_last_day AS
SELECT event_id, AVG(client_ip_last_day_eq) AS client_ip_last_day_eq_avg,
    AVG(network_last_day_eq) AS network_last_day_eq_avg,
    AVG(device_sign_last_day_eq) AS device_sign_last_day_eq_avg,
    AVG(info_1_last_day_eq) AS info_1_last_day_eq_avg,
    AVG(info_2_last_day_eq) AS info_2_last_day_eq_avg,
    AVG(ip_prov_last_day_eq) AS ip_prov_last_day_eq_avg,
    AVG(ip_city_last_day_eq) AS ip_city_last_day_eq_avg,
    AVG(cert_prov_last_day_eq) AS cert_prov_last_day_eq_avg,
    AVG(cert_city_last_day_eq) AS cert_city_last_day_eq_avg,
    AVG(card_bin_prov_last_day_eq) AS card_bin_prov_last_day_eq_avg,
    AVG(card_bin_city_last_day_eq) AS card_bin_city_last_day_eq_avg,
    AVG(card_mobile_prov_last_day_eq) AS card_mobile_prov_last_day_eq_avg,
    AVG(card_mobile_city_last_day_eq) AS card_mobile_city_last_day_eq_avg,
    AVG(card_cert_prov_last_day_eq) AS card_cert_prov_last_day_eq_avg,
    AVG(card_cert_city_last_day_eq) AS card_cert_city_last_day_eq_avg,
    AVG(is_one_people_last_day_eq) AS is_one_people_last_day_eq_avg,
    AVG(mobile_oper_platform_last_day_eq) AS mobile_oper_platform_last_day_eq_avg,
    AVG(operation_channel_last_day_eq) AS operation_channel_last_day_eq_avg,
    AVG(pay_scene_last_day_eq) AS pay_scene_last_day_eq_avg,
    AVG(amt_last_day_eq) AS amt_last_day_eq_avg,
    AVG(card_cert_no_last_day_eq) AS card_cert_no_last_day_eq_avg,
    AVG(opposing_id_last_day_eq) AS opposing_id_last_day_eq_avg,
    AVG(income_card_no_last_day_eq) AS income_card_no_last_day_eq_avg,
    AVG(income_card_cert_no_last_day_eq) AS income_card_cert_no_last_day_eq_avg,
    AVG(income_card_mobile_last_day_eq) AS income_card_mobile_last_day_eq_avg,
    AVG(income_card_bank_code_last_day_eq) AS income_card_bank_code_last_day_eq_avg,
    AVG(province_last_day_eq) AS province_last_day_eq_avg,
    AVG(city_last_day_eq) AS city_last_day_eq_avg,
    AVG(is_peer_pay_last_day_eq) AS is_peer_pay_last_day_eq_avg,
    AVG(version_last_day_eq) AS version_last_day_eq_avg,
    SUM(client_ip_last_day_eq) AS client_ip_last_day_eq_sum,
    SUM(network_last_day_eq) AS network_last_day_eq_sum,
    SUM(device_sign_last_day_eq) AS device_sign_last_day_eq_sum,
    SUM(info_1_last_day_eq) AS info_1_last_day_eq_sum,
    SUM(info_2_last_day_eq) AS info_2_last_day_eq_sum,
    SUM(ip_prov_last_day_eq) AS ip_prov_last_day_eq_sum,
    SUM(ip_city_last_day_eq) AS ip_city_last_day_eq_sum,
    SUM(cert_prov_last_day_eq) AS cert_prov_last_day_eq_sum,
    SUM(cert_city_last_day_eq) AS cert_city_last_day_eq_sum,
    SUM(card_bin_prov_last_day_eq) AS card_bin_prov_last_day_eq_sum,
    SUM(card_bin_city_last_day_eq) AS card_bin_city_last_day_eq_sum,
    SUM(card_mobile_prov_last_day_eq) AS card_mobile_prov_last_day_eq_sum,
    SUM(card_mobile_city_last_day_eq) AS card_mobile_city_last_day_eq_sum,
    SUM(card_cert_prov_last_day_eq) AS card_cert_prov_last_day_eq_sum,
    SUM(card_cert_city_last_day_eq) AS card_cert_city_last_day_eq_sum,
    SUM(is_one_people_last_day_eq) AS is_one_people_last_day_eq_sum,
    SUM(mobile_oper_platform_last_day_eq) AS mobile_oper_platform_last_day_eq_sum,
    SUM(operation_channel_last_day_eq) AS operation_channel_last_day_eq_sum,
    SUM(pay_scene_last_day_eq) AS pay_scene_last_day_eq_sum,
    SUM(amt_last_day_eq) AS amt_last_day_eq_sum,
    SUM(card_cert_no_last_day_eq) AS card_cert_no_last_day_eq_sum,
    SUM(opposing_id_last_day_eq) AS opposing_id_last_day_eq_sum,
    SUM(income_card_no_last_day_eq) AS income_card_no_last_day_eq_sum,
    SUM(income_card_cert_no_last_day_eq) AS income_card_cert_no_last_day_eq_sum,
    SUM(income_card_mobile_last_day_eq) AS income_card_mobile_last_day_eq_sum,
    SUM(income_card_bank_code_last_day_eq) AS income_card_bank_code_last_day_eq_sum,
    SUM(province_last_day_eq) AS province_last_day_eq_sum,
    SUM(city_last_day_eq) AS city_last_day_eq_sum,
    SUM(is_peer_pay_last_day_eq) AS is_peer_pay_last_day_eq_sum,
    SUM(version_last_day_eq) AS version_last_day_eq_sum
FROM temp_compare_last_day GROUP BY event_id;

-- 清除临时表
DROP TABLE IF EXISTS temp_gmt_days;
DROP TABLE IF EXISTS temp_day_order;
DROP TABLE IF EXISTS temp_compare_last_day;
