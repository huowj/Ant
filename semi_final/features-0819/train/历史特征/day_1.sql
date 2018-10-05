-- 计算gmt_days
DROP TABLE IF EXISTS temp_gmt_days_1;
CREATE TABLE IF NOT EXISTS temp_gmt_days_1 AS
SELECT a.*, datediff(concat(gmt_occur, ':00:00'), '2017-09-05 00:00:00', 'dd') as gmt_days
FROM atec_1000w_ins_data a;

-- 利用gmt_days, 构造day_order
DROP TABLE IF EXISTS temp_day_order_1;
CREATE TABLE IF NOT EXISTS temp_day_order_1 AS
SELECT t.*, dense_rank() OVER (PARTITION BY user_id ORDER BY gmt_days) day_order 
FROM temp_gmt_days_1 t;

DROP TABLE IF EXISTS temp_day_order_1_f0;
CREATE TABLE IF NOT EXISTS temp_day_order_1_f0 AS
SELECT user_id, day_order, COUNT(DISTINCT client_ip) client_ip_dount_day_order_1,
	COUNT(DISTINCT network) network_dount_day_order_1,
	COUNT(DISTINCT device_sign) device_sign_dount_day_order_1,
	COUNT(DISTINCT info_1) info_1_dount_day_order_1,
	COUNT(DISTINCT info_2) info_2_dount_day_order_1,
	COUNT(DISTINCT ip_prov) ip_prov_dount_day_order_1,
	COUNT(DISTINCT ip_city) ip_city_dount_day_order_1,
	COUNT(DISTINCT cert_prov) cert_prov_dount_day_order_1,
	COUNT(DISTINCT cert_city) cert_city_dount_day_order_1,
	COUNT(DISTINCT card_bin_prov) card_bin_prov_dount_day_order_1,
	COUNT(DISTINCT card_bin_city) card_bin_city_dount_day_order_1,
	COUNT(DISTINCT card_mobile_prov) card_mobile_prov_dount_day_order_1,
	COUNT(DISTINCT card_mobile_city) card_mobile_city_dount_day_order_1,
	COUNT(DISTINCT card_cert_prov) card_cert_prov_dount_day_order_1,
	COUNT(DISTINCT card_cert_city) card_cert_city_dount_day_order_1,
	COUNT(DISTINCT is_one_people) is_one_people_dount_day_order_1,
	COUNT(DISTINCT mobile_oper_platform) mobile_oper_platform_dount_day_order_1,
	COUNT(DISTINCT operation_channel) operation_channel_dount_day_order_1,
	COUNT(DISTINCT pay_scene) pay_scene_dount_day_order_1,
	COUNT(DISTINCT amt) amt_dount_day_order_1,
	COUNT(DISTINCT card_cert_no) card_cert_no_dount_day_order_1,
	COUNT(DISTINCT opposing_id) opposing_id_dount_day_order_1,
	COUNT(DISTINCT income_card_no) income_card_no_dount_day_order_1,
	COUNT(DISTINCT income_card_cert_no) income_card_cert_no_dount_day_order_1,
	COUNT(DISTINCT income_card_mobile) income_card_mobile_dount_day_order_1,
	COUNT(DISTINCT income_card_bank_code) income_card_bank_code_dount_day_order_1,
	COUNT(DISTINCT province) province_dount_day_order_1,
	COUNT(DISTINCT city) city_dount_day_order_1,
	COUNT(DISTINCT is_peer_pay) is_peer_pay_dount_day_order_1,
	COUNT(DISTINCT version) version_dount_day_order_1
FROM temp_day_order_1
GROUP BY user_id, day_order;

-- 30个特征(上一个有消费天内，某个字段有多少种不同的值)
DROP TABLE IF EXISTS wen_train_day_order_1_feature_0;
CREATE TABLE IF NOT EXISTS wen_train_day_order_1_feature_0 AS
SELECT event_id, client_ip_dount_day_order_1,
	network_dount_day_order_1,
	device_sign_dount_day_order_1,
	info_1_dount_day_order_1,
	info_2_dount_day_order_1,
	ip_prov_dount_day_order_1,
	ip_city_dount_day_order_1,
	cert_prov_dount_day_order_1,
	cert_city_dount_day_order_1,
	card_bin_prov_dount_day_order_1,
	card_bin_city_dount_day_order_1,
	card_mobile_prov_dount_day_order_1,
	card_mobile_city_dount_day_order_1,
	card_cert_prov_dount_day_order_1,
	card_cert_city_dount_day_order_1,
	is_one_people_dount_day_order_1,
	mobile_oper_platform_dount_day_order_1,
	operation_channel_dount_day_order_1,
	pay_scene_dount_day_order_1,
	amt_dount_day_order_1,
	card_cert_no_dount_day_order_1,
	opposing_id_dount_day_order_1,
	income_card_no_dount_day_order_1,
	income_card_cert_no_dount_day_order_1,
	income_card_mobile_dount_day_order_1,
	income_card_bank_code_dount_day_order_1,
	province_dount_day_order_1,
	city_dount_day_order_1,
	is_peer_pay_dount_day_order_1,
	version_dount_day_order_1
FROM temp_day_order_1 t1 LEFT JOIN temp_day_order_1_f0 t0
ON t1.user_id = t0.user_id AND t1.day_order = t0.day_order + 1;


-- 61个特征(上一个有消费天内，某个字段占比如何)
DROP TABLE IF EXISTS wen_train_day_order_1_feature_1;
CREATE TABLE IF NOT EXISTS wen_train_day_order_1_feature_1 AS
SELECT event_id, IF(deno_day_order_1 IS NULL, 0, deno_day_order_1) deno_day_order_1, client_ip_num_day_order_1,
	network_num_day_order_1,
	device_sign_num_day_order_1,
	info_1_num_day_order_1,
	info_2_num_day_order_1,
	ip_prov_num_day_order_1,
	ip_city_num_day_order_1,
	cert_prov_num_day_order_1,
	cert_city_num_day_order_1,
	card_bin_prov_num_day_order_1,
	card_bin_city_num_day_order_1,
	card_mobile_prov_num_day_order_1,
	card_mobile_city_num_day_order_1,
	card_cert_prov_num_day_order_1,
	card_cert_city_num_day_order_1,
	is_one_people_num_day_order_1,
	mobile_oper_platform_num_day_order_1,
	operation_channel_num_day_order_1,
	pay_scene_num_day_order_1,
	amt_num_day_order_1,
	card_cert_no_num_day_order_1,
	opposing_id_num_day_order_1,
	income_card_no_num_day_order_1,
	income_card_cert_no_num_day_order_1,
	income_card_mobile_num_day_order_1,
	income_card_bank_code_num_day_order_1,
	province_num_day_order_1,
	city_num_day_order_1,
	is_peer_pay_num_day_order_1,
	version_num_day_order_1,
	IF(deno_day_order_1 IS NULL, 0, client_ip_num_day_order_1 / deno_day_order_1) AS client_ip_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, network_num_day_order_1 / deno_day_order_1) AS network_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, device_sign_num_day_order_1 / deno_day_order_1) AS device_sign_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, info_1_num_day_order_1 / deno_day_order_1) AS info_1_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, info_2_num_day_order_1 / deno_day_order_1) AS info_2_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, ip_prov_num_day_order_1 / deno_day_order_1) AS ip_prov_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, ip_city_num_day_order_1 / deno_day_order_1) AS ip_city_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, cert_prov_num_day_order_1 / deno_day_order_1) AS cert_prov_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, cert_city_num_day_order_1 / deno_day_order_1) AS cert_city_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, card_bin_prov_num_day_order_1 / deno_day_order_1) AS card_bin_prov_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, card_bin_city_num_day_order_1 / deno_day_order_1) AS card_bin_city_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, card_mobile_prov_num_day_order_1 / deno_day_order_1) AS card_mobile_prov_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, card_mobile_city_num_day_order_1 / deno_day_order_1) AS card_mobile_city_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, card_cert_prov_num_day_order_1 / deno_day_order_1) AS card_cert_prov_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, card_cert_city_num_day_order_1 / deno_day_order_1) AS card_cert_city_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, is_one_people_num_day_order_1 / deno_day_order_1) AS is_one_people_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, mobile_oper_platform_num_day_order_1 / deno_day_order_1) AS mobile_oper_platform_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, operation_channel_num_day_order_1 / deno_day_order_1) AS operation_channel_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, pay_scene_num_day_order_1 / deno_day_order_1) AS pay_scene_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, amt_num_day_order_1 / deno_day_order_1) AS amt_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, card_cert_no_num_day_order_1 / deno_day_order_1) AS card_cert_no_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, opposing_id_num_day_order_1 / deno_day_order_1) AS opposing_id_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, income_card_no_num_day_order_1 / deno_day_order_1) AS income_card_no_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, income_card_cert_no_num_day_order_1 / deno_day_order_1) AS income_card_cert_no_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, income_card_mobile_num_day_order_1 / deno_day_order_1) AS income_card_mobile_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, income_card_bank_code_num_day_order_1 / deno_day_order_1) AS income_card_bank_code_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, province_num_day_order_1 / deno_day_order_1) AS province_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, city_num_day_order_1 / deno_day_order_1) AS city_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, is_peer_pay_num_day_order_1 / deno_day_order_1) AS is_peer_pay_rate_day_order_1,
    IF(deno_day_order_1 IS NULL, 0, version_num_day_order_1 / deno_day_order_1) AS version_rate_day_order_1
FROM temp_day_order_1 f1 
LEFT JOIN
		(SELECT user_id, day_order, COUNT(1) deno_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order) f1_0
	ON f1.user_id = f1_0.user_id AND f1.day_order = f1_0.day_order + 1
	LEFT JOIN
		(SELECT user_id, day_order, client_ip, COUNT(1) client_ip_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, client_ip) f1_client_ip
	ON f1.user_id = f1_client_ip.user_id AND f1.day_order = f1_client_ip.day_order + 1 AND f1.client_ip = f1_client_ip.client_ip
	LEFT JOIN
		(SELECT user_id, day_order, network, COUNT(1) network_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, network) f1_network
	ON f1.user_id = f1_network.user_id AND f1.day_order = f1_network.day_order + 1 AND f1.network = f1_network.network
	LEFT JOIN
		(SELECT user_id, day_order, device_sign, COUNT(1) device_sign_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, device_sign) f1_device_sign
	ON f1.user_id = f1_device_sign.user_id AND f1.day_order = f1_device_sign.day_order + 1 AND f1.device_sign = f1_device_sign.device_sign
	LEFT JOIN
		(SELECT user_id, day_order, info_1, COUNT(1) info_1_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, info_1) f1_info_1
	ON f1.user_id = f1_info_1.user_id AND f1.day_order = f1_info_1.day_order + 1 AND f1.info_1 = f1_info_1.info_1
	LEFT JOIN
		(SELECT user_id, day_order, info_2, COUNT(1) info_2_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, info_2) f1_info_2
	ON f1.user_id = f1_info_2.user_id AND f1.day_order = f1_info_2.day_order + 1 AND f1.info_2 = f1_info_2.info_2
	LEFT JOIN
		(SELECT user_id, day_order, ip_prov, COUNT(1) ip_prov_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, ip_prov) f1_ip_prov
	ON f1.user_id = f1_ip_prov.user_id AND f1.day_order = f1_ip_prov.day_order + 1 AND f1.ip_prov = f1_ip_prov.ip_prov
	LEFT JOIN
		(SELECT user_id, day_order, ip_city, COUNT(1) ip_city_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, ip_city) f1_ip_city
	ON f1.user_id = f1_ip_city.user_id AND f1.day_order = f1_ip_city.day_order + 1 AND f1.ip_city = f1_ip_city.ip_city
	LEFT JOIN
		(SELECT user_id, day_order, cert_prov, COUNT(1) cert_prov_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, cert_prov) f1_cert_prov
	ON f1.user_id = f1_cert_prov.user_id AND f1.day_order = f1_cert_prov.day_order + 1 AND f1.cert_prov = f1_cert_prov.cert_prov
	LEFT JOIN
		(SELECT user_id, day_order, cert_city, COUNT(1) cert_city_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, cert_city) f1_cert_city
	ON f1.user_id = f1_cert_city.user_id AND f1.day_order = f1_cert_city.day_order + 1 AND f1.cert_city = f1_cert_city.cert_city
	LEFT JOIN
		(SELECT user_id, day_order, card_bin_prov, COUNT(1) card_bin_prov_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, card_bin_prov) f1_card_bin_prov
	ON f1.user_id = f1_card_bin_prov.user_id AND f1.day_order = f1_card_bin_prov.day_order + 1 AND f1.card_bin_prov = f1_card_bin_prov.card_bin_prov
	LEFT JOIN
		(SELECT user_id, day_order, card_bin_city, COUNT(1) card_bin_city_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, card_bin_city) f1_card_bin_city
	ON f1.user_id = f1_card_bin_city.user_id AND f1.day_order = f1_card_bin_city.day_order + 1 AND f1.card_bin_city = f1_card_bin_city.card_bin_city
	LEFT JOIN
		(SELECT user_id, day_order, card_mobile_prov, COUNT(1) card_mobile_prov_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, card_mobile_prov) f1_card_mobile_prov
	ON f1.user_id = f1_card_mobile_prov.user_id AND f1.day_order = f1_card_mobile_prov.day_order + 1 AND f1.card_mobile_prov = f1_card_mobile_prov.card_mobile_prov
	LEFT JOIN
		(SELECT user_id, day_order, card_mobile_city, COUNT(1) card_mobile_city_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, card_mobile_city) f1_card_mobile_city
	ON f1.user_id = f1_card_mobile_city.user_id AND f1.day_order = f1_card_mobile_city.day_order + 1 AND f1.card_mobile_city = f1_card_mobile_city.card_mobile_city
	LEFT JOIN
		(SELECT user_id, day_order, card_cert_prov, COUNT(1) card_cert_prov_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, card_cert_prov) f1_card_cert_prov
	ON f1.user_id = f1_card_cert_prov.user_id AND f1.day_order = f1_card_cert_prov.day_order + 1 AND f1.card_cert_prov = f1_card_cert_prov.card_cert_prov
	LEFT JOIN
		(SELECT user_id, day_order, card_cert_city, COUNT(1) card_cert_city_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, card_cert_city) f1_card_cert_city
	ON f1.user_id = f1_card_cert_city.user_id AND f1.day_order = f1_card_cert_city.day_order + 1 AND f1.card_cert_city = f1_card_cert_city.card_cert_city
	LEFT JOIN
		(SELECT user_id, day_order, is_one_people, COUNT(1) is_one_people_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, is_one_people) f1_is_one_people
	ON f1.user_id = f1_is_one_people.user_id AND f1.day_order = f1_is_one_people.day_order + 1 AND f1.is_one_people = f1_is_one_people.is_one_people
	LEFT JOIN
		(SELECT user_id, day_order, mobile_oper_platform, COUNT(1) mobile_oper_platform_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, mobile_oper_platform) f1_mobile_oper_platform
	ON f1.user_id = f1_mobile_oper_platform.user_id AND f1.day_order = f1_mobile_oper_platform.day_order + 1 AND f1.mobile_oper_platform = f1_mobile_oper_platform.mobile_oper_platform
	LEFT JOIN
		(SELECT user_id, day_order, operation_channel, COUNT(1) operation_channel_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, operation_channel) f1_operation_channel
	ON f1.user_id = f1_operation_channel.user_id AND f1.day_order = f1_operation_channel.day_order + 1 AND f1.operation_channel = f1_operation_channel.operation_channel
	LEFT JOIN
		(SELECT user_id, day_order, pay_scene, COUNT(1) pay_scene_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, pay_scene) f1_pay_scene
	ON f1.user_id = f1_pay_scene.user_id AND f1.day_order = f1_pay_scene.day_order + 1 AND f1.pay_scene = f1_pay_scene.pay_scene
	LEFT JOIN
		(SELECT user_id, day_order, amt, COUNT(1) amt_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, amt) f1_amt
	ON f1.user_id = f1_amt.user_id AND f1.day_order = f1_amt.day_order + 1 AND f1.amt = f1_amt.amt
	LEFT JOIN
		(SELECT user_id, day_order, card_cert_no, COUNT(1) card_cert_no_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, card_cert_no) f1_card_cert_no
	ON f1.user_id = f1_card_cert_no.user_id AND f1.day_order = f1_card_cert_no.day_order + 1 AND f1.card_cert_no = f1_card_cert_no.card_cert_no
	LEFT JOIN
		(SELECT user_id, day_order, opposing_id, COUNT(1) opposing_id_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, opposing_id) f1_opposing_id
	ON f1.user_id = f1_opposing_id.user_id AND f1.day_order = f1_opposing_id.day_order + 1 AND f1.opposing_id = f1_opposing_id.opposing_id
	LEFT JOIN
		(SELECT user_id, day_order, income_card_no, COUNT(1) income_card_no_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, income_card_no) f1_income_card_no
	ON f1.user_id = f1_income_card_no.user_id AND f1.day_order = f1_income_card_no.day_order + 1 AND f1.income_card_no = f1_income_card_no.income_card_no
	LEFT JOIN
		(SELECT user_id, day_order, income_card_cert_no, COUNT(1) income_card_cert_no_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, income_card_cert_no) f1_income_card_cert_no
	ON f1.user_id = f1_income_card_cert_no.user_id AND f1.day_order = f1_income_card_cert_no.day_order + 1 AND f1.income_card_cert_no = f1_income_card_cert_no.income_card_cert_no
	LEFT JOIN
		(SELECT user_id, day_order, income_card_mobile, COUNT(1) income_card_mobile_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, income_card_mobile) f1_income_card_mobile
	ON f1.user_id = f1_income_card_mobile.user_id AND f1.day_order = f1_income_card_mobile.day_order + 1 AND f1.income_card_mobile = f1_income_card_mobile.income_card_mobile
	LEFT JOIN
		(SELECT user_id, day_order, income_card_bank_code, COUNT(1) income_card_bank_code_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, income_card_bank_code) f1_income_card_bank_code
	ON f1.user_id = f1_income_card_bank_code.user_id AND f1.day_order = f1_income_card_bank_code.day_order + 1 AND f1.income_card_bank_code = f1_income_card_bank_code.income_card_bank_code
	LEFT JOIN
		(SELECT user_id, day_order, province, COUNT(1) province_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, province) f1_province
	ON f1.user_id = f1_province.user_id AND f1.day_order = f1_province.day_order + 1 AND f1.province = f1_province.province
	LEFT JOIN
		(SELECT user_id, day_order, city, COUNT(1) city_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, city) f1_city
	ON f1.user_id = f1_city.user_id AND f1.day_order = f1_city.day_order + 1 AND f1.city = f1_city.city
	LEFT JOIN
		(SELECT user_id, day_order, is_peer_pay, COUNT(1) is_peer_pay_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, is_peer_pay) f1_is_peer_pay
	ON f1.user_id = f1_is_peer_pay.user_id AND f1.day_order = f1_is_peer_pay.day_order + 1 AND f1.is_peer_pay = f1_is_peer_pay.is_peer_pay
	LEFT JOIN
		(SELECT user_id, day_order, version, COUNT(1) version_num_day_order_1 FROM temp_day_order_1 GROUP BY user_id, day_order, version) f1_version
ON f1.user_id = f1_version.user_id AND f1.day_order = f1_version.day_order + 1 AND f1.version = f1_version.version;


-- 9个amt统计特征(上一个有消费天内)
DROP TABLE IF EXISTS wen_train_day_order_1_feature_2;
CREATE TABLE IF NOT EXISTS wen_train_day_order_1_feature_2 AS
SELECT event_id,
	amt_day_order_1_avg,
	amt_day_order_1_min,
	amt_day_order_1_max,
	amt_day_order_1_sum,
	amt_day_order_1_median,
	amt_day_order_1_stddev,
	amt_day_order_1_stddev_samp,
	amt_day_order_1_skewness,
	amt_day_order_1_kurtosis
FROM temp_day_order_1 f2
LEFT JOIN
    (
        SELECT user_id, day_order,
            AVG(amt) AS amt_day_order_1_avg, 
            MIN(amt) AS amt_day_order_1_min, 
            MAX(amt) AS amt_day_order_1_max, 
            SUM(amt) AS amt_day_order_1_sum,
            median(amt) AS amt_day_order_1_median, 
            STDDEV(amt) AS amt_day_order_1_stddev, 
            STDDEV_SAMP(amt) AS amt_day_order_1_stddev_samp,
			IF(POW(STDDEV(amt), 3) = 0, 0, AVG(POW(amt, 3))/POW(STDDEV(amt), 3)) as amt_day_order_1_skewness,
			IF(POW(STDDEV(amt), 4) = 0, 0, AVG(POW(amt, 4))/POW(STDDEV(amt), 4)) as amt_day_order_1_kurtosis
        FROM temp_day_order_1
        GROUP BY user_id, day_order
    ) t
ON f2.user_id = t.user_id AND f2.day_order = t.day_order + 1;

-- 清除临时表
DROP TABLE IF EXISTS temp_gmt_days_1;
DROP TABLE IF EXISTS temp_day_order_1;
DROP TABLE IF EXISTS temp_day_order_1_f0;
