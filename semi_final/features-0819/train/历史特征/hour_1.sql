-- 共计30+61+9=100个特征
-- wen_train_hour_1_feature_0
-- wen_train_hour_1_feature_1
-- wen_train_hour_1_feature_2

DROP TABLE IF EXISTS temp;
CREATE TABLE IF NOT EXISTS temp AS
SELECT a.*, dense_rank() OVER(PARTITION BY user_id ORDER BY gmt_occur) hour_order 
FROM atec_1000w_ins_data a;


DROP TABLE IF EXISTS tempf0;
CREATE TABLE IF NOT EXISTS tempf0 AS
SELECT user_id, hour_order, COUNT(DISTINCT client_ip) client_ip_dount_hour_1,
	COUNT(DISTINCT network) network_dount_hour_1,
	COUNT(DISTINCT device_sign) device_sign_dount_hour_1,
	COUNT(DISTINCT info_1) info_1_dount_hour_1,
	COUNT(DISTINCT info_2) info_2_dount_hour_1,
	COUNT(DISTINCT ip_prov) ip_prov_dount_hour_1,
	COUNT(DISTINCT ip_city) ip_city_dount_hour_1,
	COUNT(DISTINCT cert_prov) cert_prov_dount_hour_1,
	COUNT(DISTINCT cert_city) cert_city_dount_hour_1,
	COUNT(DISTINCT card_bin_prov) card_bin_prov_dount_hour_1,
	COUNT(DISTINCT card_bin_city) card_bin_city_dount_hour_1,
	COUNT(DISTINCT card_mobile_prov) card_mobile_prov_dount_hour_1,
	COUNT(DISTINCT card_mobile_city) card_mobile_city_dount_hour_1,
	COUNT(DISTINCT card_cert_prov) card_cert_prov_dount_hour_1,
	COUNT(DISTINCT card_cert_city) card_cert_city_dount_hour_1,
	COUNT(DISTINCT is_one_people) is_one_people_dount_hour_1,
	COUNT(DISTINCT mobile_oper_platform) mobile_oper_platform_dount_hour_1,
	COUNT(DISTINCT operation_channel) operation_channel_dount_hour_1,
	COUNT(DISTINCT pay_scene) pay_scene_dount_hour_1,
	COUNT(DISTINCT amt) amt_dount_hour_1,
	COUNT(DISTINCT card_cert_no) card_cert_no_dount_hour_1,
	COUNT(DISTINCT opposing_id) opposing_id_dount_hour_1,
	COUNT(DISTINCT income_card_no) income_card_no_dount_hour_1,
	COUNT(DISTINCT income_card_cert_no) income_card_cert_no_dount_hour_1,
	COUNT(DISTINCT income_card_mobile) income_card_mobile_dount_hour_1,
	COUNT(DISTINCT income_card_bank_code) income_card_bank_code_dount_hour_1,
	COUNT(DISTINCT province) province_dount_hour_1,
	COUNT(DISTINCT city) city_dount_hour_1,
	COUNT(DISTINCT is_peer_pay) is_peer_pay_dount_hour_1,
	COUNT(DISTINCT version) version_dount_hour_1
FROM temp
GROUP BY user_id, hour_order;


-- 30个特征(上一个消费小时内，某个字段有多少种不同的值)
DROP TABLE IF EXISTS wen_train_hour_1_feature_0;
CREATE TABLE IF NOT EXISTS wen_train_hour_1_feature_0 AS
SELECT event_id, client_ip_dount_hour_1,
	network_dount_hour_1,
	device_sign_dount_hour_1,
	info_1_dount_hour_1,
	info_2_dount_hour_1,
	ip_prov_dount_hour_1,
	ip_city_dount_hour_1,
	cert_prov_dount_hour_1,
	cert_city_dount_hour_1,
	card_bin_prov_dount_hour_1,
	card_bin_city_dount_hour_1,
	card_mobile_prov_dount_hour_1,
	card_mobile_city_dount_hour_1,
	card_cert_prov_dount_hour_1,
	card_cert_city_dount_hour_1,
	is_one_people_dount_hour_1,
	mobile_oper_platform_dount_hour_1,
	operation_channel_dount_hour_1,
	pay_scene_dount_hour_1,
	amt_dount_hour_1,
	card_cert_no_dount_hour_1,
	opposing_id_dount_hour_1,
	income_card_no_dount_hour_1,
	income_card_cert_no_dount_hour_1,
	income_card_mobile_dount_hour_1,
	income_card_bank_code_dount_hour_1,
	province_dount_hour_1,
	city_dount_hour_1,
	is_peer_pay_dount_hour_1,
	version_dount_hour_1
FROM temp t1 LEFT JOIN tempf0 t0
ON t1.user_id = t0.user_id AND t1.hour_order = t0.hour_order + 1;


-- 61个特征(上一个消费小时内，某个字段占比如何)
DROP TABLE IF EXISTS wen_train_hour_1_feature_1;
CREATE TABLE IF NOT EXISTS wen_train_hour_1_feature_1 AS
SELECT event_id, IF(deno_hour_1 IS NULL, 0, deno_hour_1) AS deno_hour_1, 
	client_ip_num_hour_1,
	network_num_hour_1,
	device_sign_num_hour_1,
	info_1_num_hour_1,
	info_2_num_hour_1,
	ip_prov_num_hour_1,
	ip_city_num_hour_1,
	cert_prov_num_hour_1,
	cert_city_num_hour_1,
	card_bin_prov_num_hour_1,
	card_bin_city_num_hour_1,
	card_mobile_prov_num_hour_1,
	card_mobile_city_num_hour_1,
	card_cert_prov_num_hour_1,
	card_cert_city_num_hour_1,
	is_one_people_num_hour_1,
	mobile_oper_platform_num_hour_1,
	operation_channel_num_hour_1,
	pay_scene_num_hour_1,
	amt_num_hour_1,
	card_cert_no_num_hour_1,
	opposing_id_num_hour_1,
	income_card_no_num_hour_1,
	income_card_cert_no_num_hour_1,
	income_card_mobile_num_hour_1,
	income_card_bank_code_num_hour_1,
	province_num_hour_1,
	city_num_hour_1,
	is_peer_pay_num_hour_1,
	version_num_hour_1,
	IF(deno_hour_1 IS NULL, 0, client_ip_num_hour_1 / deno_hour_1) AS client_ip_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, network_num_hour_1 / deno_hour_1) AS network_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, device_sign_num_hour_1 / deno_hour_1) AS device_sign_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, info_1_num_hour_1 / deno_hour_1) AS info_1_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, info_2_num_hour_1 / deno_hour_1) AS info_2_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, ip_prov_num_hour_1 / deno_hour_1) AS ip_prov_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, ip_city_num_hour_1 / deno_hour_1) AS ip_city_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, cert_prov_num_hour_1 / deno_hour_1) AS cert_prov_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, cert_city_num_hour_1 / deno_hour_1) AS cert_city_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, card_bin_prov_num_hour_1 / deno_hour_1) AS card_bin_prov_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, card_bin_city_num_hour_1 / deno_hour_1) AS card_bin_city_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, card_mobile_prov_num_hour_1 / deno_hour_1) AS card_mobile_prov_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, card_mobile_city_num_hour_1 / deno_hour_1) AS card_mobile_city_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, card_cert_prov_num_hour_1 / deno_hour_1) AS card_cert_prov_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, card_cert_city_num_hour_1 / deno_hour_1) AS card_cert_city_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, is_one_people_num_hour_1 / deno_hour_1) AS is_one_people_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, mobile_oper_platform_num_hour_1 / deno_hour_1) AS mobile_oper_platform_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, operation_channel_num_hour_1 / deno_hour_1) AS operation_channel_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, pay_scene_num_hour_1 / deno_hour_1) AS pay_scene_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, amt_num_hour_1 / deno_hour_1) AS amt_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, card_cert_no_num_hour_1 / deno_hour_1) AS card_cert_no_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, opposing_id_num_hour_1 / deno_hour_1) AS opposing_id_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, income_card_no_num_hour_1 / deno_hour_1) AS income_card_no_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, income_card_cert_no_num_hour_1 / deno_hour_1) AS income_card_cert_no_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, income_card_mobile_num_hour_1 / deno_hour_1) AS income_card_mobile_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, income_card_bank_code_num_hour_1 / deno_hour_1) AS income_card_bank_code_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, province_num_hour_1 / deno_hour_1) AS province_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, city_num_hour_1 / deno_hour_1) AS city_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, is_peer_pay_num_hour_1 / deno_hour_1) AS is_peer_pay_rate_hour_1,
	IF(deno_hour_1 IS NULL, 0, version_num_hour_1 / deno_hour_1) AS version_rate_hour_1
FROM temp f1 
LEFT JOIN
		(SELECT user_id, hour_order, COUNT(1) deno_hour_1 FROM temp GROUP BY user_id, hour_order) f1_0
	ON f1.user_id = f1_0.user_id AND f1.hour_order = f1_0.hour_order + 1
	LEFT JOIN
		(SELECT user_id, hour_order, client_ip, COUNT(1) client_ip_num_hour_1 FROM temp GROUP BY user_id, hour_order, client_ip) f1_client_ip
	ON f1.user_id = f1_client_ip.user_id AND f1.hour_order = f1_client_ip.hour_order + 1 AND f1.client_ip = f1_client_ip.client_ip
	LEFT JOIN
		(SELECT user_id, hour_order, network, COUNT(1) network_num_hour_1 FROM temp GROUP BY user_id, hour_order, network) f1_network
	ON f1.user_id = f1_network.user_id AND f1.hour_order = f1_network.hour_order + 1 AND f1.network = f1_network.network
	LEFT JOIN
		(SELECT user_id, hour_order, device_sign, COUNT(1) device_sign_num_hour_1 FROM temp GROUP BY user_id, hour_order, device_sign) f1_device_sign
	ON f1.user_id = f1_device_sign.user_id AND f1.hour_order = f1_device_sign.hour_order + 1 AND f1.device_sign = f1_device_sign.device_sign
	LEFT JOIN
		(SELECT user_id, hour_order, info_1, COUNT(1) info_1_num_hour_1 FROM temp GROUP BY user_id, hour_order, info_1) f1_info_1
	ON f1.user_id = f1_info_1.user_id AND f1.hour_order = f1_info_1.hour_order + 1 AND f1.info_1 = f1_info_1.info_1
	LEFT JOIN
		(SELECT user_id, hour_order, info_2, COUNT(1) info_2_num_hour_1 FROM temp GROUP BY user_id, hour_order, info_2) f1_info_2
	ON f1.user_id = f1_info_2.user_id AND f1.hour_order = f1_info_2.hour_order + 1 AND f1.info_2 = f1_info_2.info_2
	LEFT JOIN
		(SELECT user_id, hour_order, ip_prov, COUNT(1) ip_prov_num_hour_1 FROM temp GROUP BY user_id, hour_order, ip_prov) f1_ip_prov
	ON f1.user_id = f1_ip_prov.user_id AND f1.hour_order = f1_ip_prov.hour_order + 1 AND f1.ip_prov = f1_ip_prov.ip_prov
	LEFT JOIN
		(SELECT user_id, hour_order, ip_city, COUNT(1) ip_city_num_hour_1 FROM temp GROUP BY user_id, hour_order, ip_city) f1_ip_city
	ON f1.user_id = f1_ip_city.user_id AND f1.hour_order = f1_ip_city.hour_order + 1 AND f1.ip_city = f1_ip_city.ip_city
	LEFT JOIN
		(SELECT user_id, hour_order, cert_prov, COUNT(1) cert_prov_num_hour_1 FROM temp GROUP BY user_id, hour_order, cert_prov) f1_cert_prov
	ON f1.user_id = f1_cert_prov.user_id AND f1.hour_order = f1_cert_prov.hour_order + 1 AND f1.cert_prov = f1_cert_prov.cert_prov
	LEFT JOIN
		(SELECT user_id, hour_order, cert_city, COUNT(1) cert_city_num_hour_1 FROM temp GROUP BY user_id, hour_order, cert_city) f1_cert_city
	ON f1.user_id = f1_cert_city.user_id AND f1.hour_order = f1_cert_city.hour_order + 1 AND f1.cert_city = f1_cert_city.cert_city
	LEFT JOIN
		(SELECT user_id, hour_order, card_bin_prov, COUNT(1) card_bin_prov_num_hour_1 FROM temp GROUP BY user_id, hour_order, card_bin_prov) f1_card_bin_prov
	ON f1.user_id = f1_card_bin_prov.user_id AND f1.hour_order = f1_card_bin_prov.hour_order + 1 AND f1.card_bin_prov = f1_card_bin_prov.card_bin_prov
	LEFT JOIN
		(SELECT user_id, hour_order, card_bin_city, COUNT(1) card_bin_city_num_hour_1 FROM temp GROUP BY user_id, hour_order, card_bin_city) f1_card_bin_city
	ON f1.user_id = f1_card_bin_city.user_id AND f1.hour_order = f1_card_bin_city.hour_order + 1 AND f1.card_bin_city = f1_card_bin_city.card_bin_city
	LEFT JOIN
		(SELECT user_id, hour_order, card_mobile_prov, COUNT(1) card_mobile_prov_num_hour_1 FROM temp GROUP BY user_id, hour_order, card_mobile_prov) f1_card_mobile_prov
	ON f1.user_id = f1_card_mobile_prov.user_id AND f1.hour_order = f1_card_mobile_prov.hour_order + 1 AND f1.card_mobile_prov = f1_card_mobile_prov.card_mobile_prov
	LEFT JOIN
		(SELECT user_id, hour_order, card_mobile_city, COUNT(1) card_mobile_city_num_hour_1 FROM temp GROUP BY user_id, hour_order, card_mobile_city) f1_card_mobile_city
	ON f1.user_id = f1_card_mobile_city.user_id AND f1.hour_order = f1_card_mobile_city.hour_order + 1 AND f1.card_mobile_city = f1_card_mobile_city.card_mobile_city
	LEFT JOIN
		(SELECT user_id, hour_order, card_cert_prov, COUNT(1) card_cert_prov_num_hour_1 FROM temp GROUP BY user_id, hour_order, card_cert_prov) f1_card_cert_prov
	ON f1.user_id = f1_card_cert_prov.user_id AND f1.hour_order = f1_card_cert_prov.hour_order + 1 AND f1.card_cert_prov = f1_card_cert_prov.card_cert_prov
	LEFT JOIN
		(SELECT user_id, hour_order, card_cert_city, COUNT(1) card_cert_city_num_hour_1 FROM temp GROUP BY user_id, hour_order, card_cert_city) f1_card_cert_city
	ON f1.user_id = f1_card_cert_city.user_id AND f1.hour_order = f1_card_cert_city.hour_order + 1 AND f1.card_cert_city = f1_card_cert_city.card_cert_city
	LEFT JOIN
		(SELECT user_id, hour_order, is_one_people, COUNT(1) is_one_people_num_hour_1 FROM temp GROUP BY user_id, hour_order, is_one_people) f1_is_one_people
	ON f1.user_id = f1_is_one_people.user_id AND f1.hour_order = f1_is_one_people.hour_order + 1 AND f1.is_one_people = f1_is_one_people.is_one_people
	LEFT JOIN
		(SELECT user_id, hour_order, mobile_oper_platform, COUNT(1) mobile_oper_platform_num_hour_1 FROM temp GROUP BY user_id, hour_order, mobile_oper_platform) f1_mobile_oper_platform
	ON f1.user_id = f1_mobile_oper_platform.user_id AND f1.hour_order = f1_mobile_oper_platform.hour_order + 1 AND f1.mobile_oper_platform = f1_mobile_oper_platform.mobile_oper_platform
	LEFT JOIN
		(SELECT user_id, hour_order, operation_channel, COUNT(1) operation_channel_num_hour_1 FROM temp GROUP BY user_id, hour_order, operation_channel) f1_operation_channel
	ON f1.user_id = f1_operation_channel.user_id AND f1.hour_order = f1_operation_channel.hour_order + 1 AND f1.operation_channel = f1_operation_channel.operation_channel
	LEFT JOIN
		(SELECT user_id, hour_order, pay_scene, COUNT(1) pay_scene_num_hour_1 FROM temp GROUP BY user_id, hour_order, pay_scene) f1_pay_scene
	ON f1.user_id = f1_pay_scene.user_id AND f1.hour_order = f1_pay_scene.hour_order + 1 AND f1.pay_scene = f1_pay_scene.pay_scene
	LEFT JOIN
		(SELECT user_id, hour_order, amt, COUNT(1) amt_num_hour_1 FROM temp GROUP BY user_id, hour_order, amt) f1_amt
	ON f1.user_id = f1_amt.user_id AND f1.hour_order = f1_amt.hour_order + 1 AND f1.amt = f1_amt.amt
	LEFT JOIN
		(SELECT user_id, hour_order, card_cert_no, COUNT(1) card_cert_no_num_hour_1 FROM temp GROUP BY user_id, hour_order, card_cert_no) f1_card_cert_no
	ON f1.user_id = f1_card_cert_no.user_id AND f1.hour_order = f1_card_cert_no.hour_order + 1 AND f1.card_cert_no = f1_card_cert_no.card_cert_no
	LEFT JOIN
		(SELECT user_id, hour_order, opposing_id, COUNT(1) opposing_id_num_hour_1 FROM temp GROUP BY user_id, hour_order, opposing_id) f1_opposing_id
	ON f1.user_id = f1_opposing_id.user_id AND f1.hour_order = f1_opposing_id.hour_order + 1 AND f1.opposing_id = f1_opposing_id.opposing_id
	LEFT JOIN
		(SELECT user_id, hour_order, income_card_no, COUNT(1) income_card_no_num_hour_1 FROM temp GROUP BY user_id, hour_order, income_card_no) f1_income_card_no
	ON f1.user_id = f1_income_card_no.user_id AND f1.hour_order = f1_income_card_no.hour_order + 1 AND f1.income_card_no = f1_income_card_no.income_card_no
	LEFT JOIN
		(SELECT user_id, hour_order, income_card_cert_no, COUNT(1) income_card_cert_no_num_hour_1 FROM temp GROUP BY user_id, hour_order, income_card_cert_no) f1_income_card_cert_no
	ON f1.user_id = f1_income_card_cert_no.user_id AND f1.hour_order = f1_income_card_cert_no.hour_order + 1 AND f1.income_card_cert_no = f1_income_card_cert_no.income_card_cert_no
	LEFT JOIN
		(SELECT user_id, hour_order, income_card_mobile, COUNT(1) income_card_mobile_num_hour_1 FROM temp GROUP BY user_id, hour_order, income_card_mobile) f1_income_card_mobile
	ON f1.user_id = f1_income_card_mobile.user_id AND f1.hour_order = f1_income_card_mobile.hour_order + 1 AND f1.income_card_mobile = f1_income_card_mobile.income_card_mobile
	LEFT JOIN
		(SELECT user_id, hour_order, income_card_bank_code, COUNT(1) income_card_bank_code_num_hour_1 FROM temp GROUP BY user_id, hour_order, income_card_bank_code) f1_income_card_bank_code
	ON f1.user_id = f1_income_card_bank_code.user_id AND f1.hour_order = f1_income_card_bank_code.hour_order + 1 AND f1.income_card_bank_code = f1_income_card_bank_code.income_card_bank_code
	LEFT JOIN
		(SELECT user_id, hour_order, province, COUNT(1) province_num_hour_1 FROM temp GROUP BY user_id, hour_order, province) f1_province
	ON f1.user_id = f1_province.user_id AND f1.hour_order = f1_province.hour_order + 1 AND f1.province = f1_province.province
	LEFT JOIN
		(SELECT user_id, hour_order, city, COUNT(1) city_num_hour_1 FROM temp GROUP BY user_id, hour_order, city) f1_city
	ON f1.user_id = f1_city.user_id AND f1.hour_order = f1_city.hour_order + 1 AND f1.city = f1_city.city
	LEFT JOIN
		(SELECT user_id, hour_order, is_peer_pay, COUNT(1) is_peer_pay_num_hour_1 FROM temp GROUP BY user_id, hour_order, is_peer_pay) f1_is_peer_pay
	ON f1.user_id = f1_is_peer_pay.user_id AND f1.hour_order = f1_is_peer_pay.hour_order + 1 AND f1.is_peer_pay = f1_is_peer_pay.is_peer_pay
	LEFT JOIN
		(SELECT user_id, hour_order, version, COUNT(1) version_num_hour_1 FROM temp GROUP BY user_id, hour_order, version) f1_version
ON f1.user_id = f1_version.user_id AND f1.hour_order = f1_version.hour_order + 1 AND f1.version = f1_version.version;


-- 9个amt统计特征(上一个消费小时)
DROP TABLE IF EXISTS wen_train_hour_1_feature_2;
CREATE TABLE IF NOT EXISTS wen_train_hour_1_feature_2 AS
SELECT event_id,
	amt_hour_1_avg,
	amt_hour_1_min,
	amt_hour_1_max,
	amt_hour_1_sum,
	amt_hour_1_median,
	amt_hour_1_stddev,
	amt_hour_1_stddev_samp,
	amt_hour_1_skewness,
	amt_hour_1_kurtosis
FROM temp f2
LEFT JOIN
    (
        SELECT user_id, hour_order,
            AVG(amt) AS amt_hour_1_avg, 
            MIN(amt) AS amt_hour_1_min, 
            MAX(amt) AS amt_hour_1_max, 
            SUM(amt) AS amt_hour_1_sum,
            median(amt) AS amt_hour_1_median, 
            STDDEV(amt) AS amt_hour_1_stddev, 
            STDDEV_SAMP(amt) AS amt_hour_1_stddev_samp,
			IF(POW(STDDEV(amt), 3) = 0, 0, AVG(POW(amt, 3))/POW(STDDEV(amt), 3)) as amt_hour_1_skewness,
			IF(POW(STDDEV(amt), 4) = 0, 0, AVG(POW(amt, 4))/POW(STDDEV(amt), 4)) as amt_hour_1_kurtosis
        FROM temp
        GROUP BY user_id, hour_order
    ) t
ON f2.user_id = t.user_id AND f2.hour_order = t.hour_order + 1;


-- 清除本文件中的中间表
DROP TABLE IF EXISTS temp;
DROP TABLE IF EXISTS tempf0;
