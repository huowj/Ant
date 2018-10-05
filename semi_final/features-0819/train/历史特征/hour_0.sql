-- 共计30+61+9=100个特征

-- wen_train_hour_0_feature_0
-- wen_train_hour_0_feature_1
-- wen_train_hour_0_feature_2

-- 该表用来帮助构建此文件中的表wen_train_feature_0
DROP TABLE IF EXISTS temp0;
CREATE TABLE IF NOT EXISTS temp0 AS
SELECT user_id, gmt_occur,
	COUNT(DISTINCT client_ip) client_ip_dcount,
	COUNT(DISTINCT network) network_dcount,
	COUNT(DISTINCT device_sign) device_sign_dcount,
	COUNT(DISTINCT info_1) info_1_dcount,
	COUNT(DISTINCT info_2) info_2_dcount,
	COUNT(DISTINCT ip_prov) ip_prov_dcount,
	COUNT(DISTINCT ip_city) ip_city_dcount,
	COUNT(DISTINCT cert_prov) cert_prov_dcount,
	COUNT(DISTINCT cert_city) cert_city_dcount,
	COUNT(DISTINCT card_bin_prov) card_bin_prov_dcount,
	COUNT(DISTINCT card_bin_city) card_bin_city_dcount,
	COUNT(DISTINCT card_mobile_prov) card_mobile_prov_dcount,
	COUNT(DISTINCT card_mobile_city) card_mobile_city_dcount,
	COUNT(DISTINCT card_cert_prov) card_cert_prov_dcount,
	COUNT(DISTINCT card_cert_city) card_cert_city_dcount,
	COUNT(DISTINCT is_one_people) is_one_people_dcount,
	COUNT(DISTINCT mobile_oper_platform) mobile_oper_platform_dcount,
	COUNT(DISTINCT operation_channel) operation_channel_dcount,
	COUNT(DISTINCT pay_scene) pay_scene_dcount,
	COUNT(DISTINCT amt) amt_dcount,
	COUNT(DISTINCT card_cert_no) card_cert_no_dcount,
	COUNT(DISTINCT opposing_id) opposing_id_dcount,
	COUNT(DISTINCT income_card_no) income_card_no_dcount,
	COUNT(DISTINCT income_card_cert_no) income_card_cert_no_dcount,
	COUNT(DISTINCT income_card_mobile) income_card_mobile_dcount,
	COUNT(DISTINCT income_card_bank_code) income_card_bank_code_dcount,
	COUNT(DISTINCT province) province_dcount,
	COUNT(DISTINCT city) city_dcount,
	COUNT(DISTINCT is_peer_pay) is_peer_pay_dcount,
	COUNT(DISTINCT version) version_dcount
FROM atec_1000w_ins_data
GROUP BY user_id, gmt_occur;


-- 30个特征(该小时内，某个字段有多少种不同的值)
DROP TABLE IF EXISTS wen_train_hour_0_feature_0;
CREATE TABLE IF NOT EXISTS wen_train_hour_0_feature_0 AS
SELECT event_id, client_ip_dcount,
	network_dcount,
	device_sign_dcount,
	info_1_dcount,
	info_2_dcount,
	ip_prov_dcount,
	ip_city_dcount,
	cert_prov_dcount,
	cert_city_dcount,
	card_bin_prov_dcount,
	card_bin_city_dcount,
	card_mobile_prov_dcount,
	card_mobile_city_dcount,
	card_cert_prov_dcount,
	card_cert_city_dcount,
	is_one_people_dcount,
	mobile_oper_platform_dcount,
	operation_channel_dcount,
	pay_scene_dcount,
	amt_dcount,
	card_cert_no_dcount,
	opposing_id_dcount,
	income_card_no_dcount,
	income_card_cert_no_dcount,
	income_card_mobile_dcount,
	income_card_bank_code_dcount,
	province_dcount,
	city_dcount,
	is_peer_pay_dcount,
	version_dcount
FROM atec_1000w_ins_data t LEFT JOIN temp0
ON t.user_id = temp0.user_id AND t.gmt_occur = temp0.gmt_occur;


-- 61个特征(该小时内，某个字段占比如何)
DROP TABLE IF EXISTS wen_train_hour_0_feature_1;
CREATE TABLE IF NOT EXISTS wen_train_hour_0_feature_1 AS
SELECT event_id, deno,
	client_ip_num,
	network_num,
	device_sign_num,
	info_1_num,
	info_2_num,
	ip_prov_num,
	ip_city_num,
	cert_prov_num,
	cert_city_num,
	card_bin_prov_num,
	card_bin_city_num,
	card_mobile_prov_num,
	card_mobile_city_num,
	card_cert_prov_num,
	card_cert_city_num,
	is_one_people_num,
	mobile_oper_platform_num,
	operation_channel_num,
	pay_scene_num,
	amt_num,
	card_cert_no_num,
	opposing_id_num,
	income_card_no_num,
	income_card_cert_no_num,
	income_card_mobile_num,
	income_card_bank_code_num,
	province_num,
	city_num,
	is_peer_pay_num,
	version_num,
	client_ip_num / deno AS client_ip_rate,
	network_num / deno AS network_rate,
	device_sign_num / deno AS device_sign_rate,
	info_1_num / deno AS info_1_rate,
	info_2_num / deno AS info_2_rate,
	ip_prov_num / deno AS ip_prov_rate,
	ip_city_num / deno AS ip_city_rate,
	cert_prov_num / deno AS cert_prov_rate,
	cert_city_num / deno AS cert_city_rate,
	card_bin_prov_num / deno AS card_bin_prov_rate,
	card_bin_city_num / deno AS card_bin_city_rate,
	card_mobile_prov_num / deno AS card_mobile_prov_rate,
	card_mobile_city_num / deno AS card_mobile_city_rate,
	card_cert_prov_num / deno AS card_cert_prov_rate,
	card_cert_city_num / deno AS card_cert_city_rate,
	is_one_people_num / deno AS is_one_people_rate,
	mobile_oper_platform_num / deno AS mobile_oper_platform_rate,
	operation_channel_num / deno AS operation_channel_rate,
	pay_scene_num / deno AS pay_scene_rate,
	amt_num / deno AS amt_rate,
	card_cert_no_num / deno AS card_cert_no_rate,
	opposing_id_num / deno AS opposing_id_rate,
	income_card_no_num / deno AS income_card_no_rate,
	income_card_cert_no_num / deno AS income_card_cert_no_rate,
	income_card_mobile_num / deno AS income_card_mobile_rate,
	income_card_bank_code_num / deno AS income_card_bank_code_rate,
	province_num / deno AS province_rate,
	city_num / deno AS city_rate,
	is_peer_pay_num / deno AS is_peer_pay_rate,
	version_num / deno AS version_rate
FROM atec_1000w_ins_data f1 
	LEFT JOIN
		(SELECT user_id, gmt_occur, COUNT(1) deno FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur) f1_0
	ON f1.user_id = f1_0.user_id AND f1.gmt_occur = f1_0.gmt_occur
	LEFT JOIN
		(SELECT user_id, gmt_occur, client_ip, COUNT(1) client_ip_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, client_ip) f1_client_ip
	ON f1.user_id = f1_client_ip.user_id AND f1.gmt_occur = f1_client_ip.gmt_occur AND f1.client_ip = f1_client_ip.client_ip
	LEFT JOIN
		(SELECT user_id, gmt_occur, network, COUNT(1) network_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, network) f1_network
	ON f1.user_id = f1_network.user_id AND f1.gmt_occur = f1_network.gmt_occur AND f1.network = f1_network.network
	LEFT JOIN
		(SELECT user_id, gmt_occur, device_sign, COUNT(1) device_sign_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, device_sign) f1_device_sign
	ON f1.user_id = f1_device_sign.user_id AND f1.gmt_occur = f1_device_sign.gmt_occur AND f1.device_sign = f1_device_sign.device_sign
	LEFT JOIN
		(SELECT user_id, gmt_occur, info_1, COUNT(1) info_1_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, info_1) f1_info_1
	ON f1.user_id = f1_info_1.user_id AND f1.gmt_occur = f1_info_1.gmt_occur AND f1.info_1 = f1_info_1.info_1
	LEFT JOIN
		(SELECT user_id, gmt_occur, info_2, COUNT(1) info_2_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, info_2) f1_info_2
	ON f1.user_id = f1_info_2.user_id AND f1.gmt_occur = f1_info_2.gmt_occur AND f1.info_2 = f1_info_2.info_2
	LEFT JOIN
		(SELECT user_id, gmt_occur, ip_prov, COUNT(1) ip_prov_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, ip_prov) f1_ip_prov
	ON f1.user_id = f1_ip_prov.user_id AND f1.gmt_occur = f1_ip_prov.gmt_occur AND f1.ip_prov = f1_ip_prov.ip_prov
	LEFT JOIN
		(SELECT user_id, gmt_occur, ip_city, COUNT(1) ip_city_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, ip_city) f1_ip_city
	ON f1.user_id = f1_ip_city.user_id AND f1.gmt_occur = f1_ip_city.gmt_occur AND f1.ip_city = f1_ip_city.ip_city
	LEFT JOIN
		(SELECT user_id, gmt_occur, cert_prov, COUNT(1) cert_prov_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, cert_prov) f1_cert_prov
	ON f1.user_id = f1_cert_prov.user_id AND f1.gmt_occur = f1_cert_prov.gmt_occur AND f1.cert_prov = f1_cert_prov.cert_prov
	LEFT JOIN
		(SELECT user_id, gmt_occur, cert_city, COUNT(1) cert_city_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, cert_city) f1_cert_city
	ON f1.user_id = f1_cert_city.user_id AND f1.gmt_occur = f1_cert_city.gmt_occur AND f1.cert_city = f1_cert_city.cert_city
	LEFT JOIN
		(SELECT user_id, gmt_occur, card_bin_prov, COUNT(1) card_bin_prov_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, card_bin_prov) f1_card_bin_prov
	ON f1.user_id = f1_card_bin_prov.user_id AND f1.gmt_occur = f1_card_bin_prov.gmt_occur AND f1.card_bin_prov = f1_card_bin_prov.card_bin_prov
	LEFT JOIN
		(SELECT user_id, gmt_occur, card_bin_city, COUNT(1) card_bin_city_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, card_bin_city) f1_card_bin_city
	ON f1.user_id = f1_card_bin_city.user_id AND f1.gmt_occur = f1_card_bin_city.gmt_occur AND f1.card_bin_city = f1_card_bin_city.card_bin_city
	LEFT JOIN
		(SELECT user_id, gmt_occur, card_mobile_prov, COUNT(1) card_mobile_prov_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, card_mobile_prov) f1_card_mobile_prov
	ON f1.user_id = f1_card_mobile_prov.user_id AND f1.gmt_occur = f1_card_mobile_prov.gmt_occur AND f1.card_mobile_prov = f1_card_mobile_prov.card_mobile_prov
	LEFT JOIN
		(SELECT user_id, gmt_occur, card_mobile_city, COUNT(1) card_mobile_city_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, card_mobile_city) f1_card_mobile_city
	ON f1.user_id = f1_card_mobile_city.user_id AND f1.gmt_occur = f1_card_mobile_city.gmt_occur AND f1.card_mobile_city = f1_card_mobile_city.card_mobile_city
	LEFT JOIN
		(SELECT user_id, gmt_occur, card_cert_prov, COUNT(1) card_cert_prov_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, card_cert_prov) f1_card_cert_prov
	ON f1.user_id = f1_card_cert_prov.user_id AND f1.gmt_occur = f1_card_cert_prov.gmt_occur AND f1.card_cert_prov = f1_card_cert_prov.card_cert_prov
	LEFT JOIN
		(SELECT user_id, gmt_occur, card_cert_city, COUNT(1) card_cert_city_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, card_cert_city) f1_card_cert_city
	ON f1.user_id = f1_card_cert_city.user_id AND f1.gmt_occur = f1_card_cert_city.gmt_occur AND f1.card_cert_city = f1_card_cert_city.card_cert_city
	LEFT JOIN
		(SELECT user_id, gmt_occur, is_one_people, COUNT(1) is_one_people_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, is_one_people) f1_is_one_people
	ON f1.user_id = f1_is_one_people.user_id AND f1.gmt_occur = f1_is_one_people.gmt_occur AND f1.is_one_people = f1_is_one_people.is_one_people
	LEFT JOIN
		(SELECT user_id, gmt_occur, mobile_oper_platform, COUNT(1) mobile_oper_platform_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, mobile_oper_platform) f1_mobile_oper_platform
	ON f1.user_id = f1_mobile_oper_platform.user_id AND f1.gmt_occur = f1_mobile_oper_platform.gmt_occur AND f1.mobile_oper_platform = f1_mobile_oper_platform.mobile_oper_platform
	LEFT JOIN
		(SELECT user_id, gmt_occur, operation_channel, COUNT(1) operation_channel_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, operation_channel) f1_operation_channel
	ON f1.user_id = f1_operation_channel.user_id AND f1.gmt_occur = f1_operation_channel.gmt_occur AND f1.operation_channel = f1_operation_channel.operation_channel
	LEFT JOIN
		(SELECT user_id, gmt_occur, pay_scene, COUNT(1) pay_scene_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, pay_scene) f1_pay_scene
	ON f1.user_id = f1_pay_scene.user_id AND f1.gmt_occur = f1_pay_scene.gmt_occur AND f1.pay_scene = f1_pay_scene.pay_scene
	LEFT JOIN
		(SELECT user_id, gmt_occur, amt, COUNT(1) amt_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, amt) f1_amt
	ON f1.user_id = f1_amt.user_id AND f1.gmt_occur = f1_amt.gmt_occur AND f1.amt = f1_amt.amt
	LEFT JOIN
		(SELECT user_id, gmt_occur, card_cert_no, COUNT(1) card_cert_no_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, card_cert_no) f1_card_cert_no
	ON f1.user_id = f1_card_cert_no.user_id AND f1.gmt_occur = f1_card_cert_no.gmt_occur AND f1.card_cert_no = f1_card_cert_no.card_cert_no
	LEFT JOIN
		(SELECT user_id, gmt_occur, opposing_id, COUNT(1) opposing_id_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, opposing_id) f1_opposing_id
	ON f1.user_id = f1_opposing_id.user_id AND f1.gmt_occur = f1_opposing_id.gmt_occur AND f1.opposing_id = f1_opposing_id.opposing_id
	LEFT JOIN
		(SELECT user_id, gmt_occur, income_card_no, COUNT(1) income_card_no_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, income_card_no) f1_income_card_no
	ON f1.user_id = f1_income_card_no.user_id AND f1.gmt_occur = f1_income_card_no.gmt_occur AND f1.income_card_no = f1_income_card_no.income_card_no
	LEFT JOIN
		(SELECT user_id, gmt_occur, income_card_cert_no, COUNT(1) income_card_cert_no_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, income_card_cert_no) f1_income_card_cert_no
	ON f1.user_id = f1_income_card_cert_no.user_id AND f1.gmt_occur = f1_income_card_cert_no.gmt_occur AND f1.income_card_cert_no = f1_income_card_cert_no.income_card_cert_no
	LEFT JOIN
		(SELECT user_id, gmt_occur, income_card_mobile, COUNT(1) income_card_mobile_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, income_card_mobile) f1_income_card_mobile
	ON f1.user_id = f1_income_card_mobile.user_id AND f1.gmt_occur = f1_income_card_mobile.gmt_occur AND f1.income_card_mobile = f1_income_card_mobile.income_card_mobile
	LEFT JOIN
		(SELECT user_id, gmt_occur, income_card_bank_code, COUNT(1) income_card_bank_code_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, income_card_bank_code) f1_income_card_bank_code
	ON f1.user_id = f1_income_card_bank_code.user_id AND f1.gmt_occur = f1_income_card_bank_code.gmt_occur AND f1.income_card_bank_code = f1_income_card_bank_code.income_card_bank_code
	LEFT JOIN
		(SELECT user_id, gmt_occur, province, COUNT(1) province_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, province) f1_province
	ON f1.user_id = f1_province.user_id AND f1.gmt_occur = f1_province.gmt_occur AND f1.province = f1_province.province
	LEFT JOIN
		(SELECT user_id, gmt_occur, city, COUNT(1) city_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, city) f1_city
	ON f1.user_id = f1_city.user_id AND f1.gmt_occur = f1_city.gmt_occur AND f1.city = f1_city.city
	LEFT JOIN
		(SELECT user_id, gmt_occur, is_peer_pay, COUNT(1) is_peer_pay_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, is_peer_pay) f1_is_peer_pay
	ON f1.user_id = f1_is_peer_pay.user_id AND f1.gmt_occur = f1_is_peer_pay.gmt_occur AND f1.is_peer_pay = f1_is_peer_pay.is_peer_pay
	LEFT JOIN
		(SELECT user_id, gmt_occur, version, COUNT(1) version_num FROM atec_1000w_ins_data GROUP BY user_id, gmt_occur, version) f1_version
ON f1.user_id = f1_version.user_id AND f1.gmt_occur = f1_version.gmt_occur AND f1.version = f1_version.version;


-- 9个amt统计特征
DROP TABLE IF EXISTS wen_train_hour_0_feature_2;
CREATE TABLE IF NOT EXISTS wen_train_hour_0_feature_2 AS
SELECT event_id,
	amt_avg,
	amt_min,
	amt_max,
	amt_sum,
	amt_median,
	amt_stddev,
	amt_stddev_samp,
	amt_skewness,
	amt_kurtosis
FROM atec_1000w_ins_data f2
LEFT JOIN
	(
		SELECT user_id, gmt_occur,
			AVG(amt) AS amt_avg, 
			MIN(amt) AS amt_min, 
			MAX(amt) AS amt_max, 
			SUM(amt) AS amt_sum,
			median(amt) AS amt_median, 
			STDDEV(amt) AS amt_stddev, 
			STDDEV_SAMP(amt) AS amt_stddev_samp,
			IF(POW(STDDEV(amt), 3) = 0, 0, AVG(POW(amt, 3))/POW(STDDEV(amt), 3)) as amt_skewness,
			IF(POW(STDDEV(amt), 4) = 0, 0, AVG(POW(amt, 4))/POW(STDDEV(amt), 4)) as amt_kurtosis
		FROM atec_1000w_ins_data
		GROUP BY user_id, gmt_occur
	) temp
ON f2.user_id = temp.user_id AND f2.gmt_occur = temp.gmt_occur;


-- 清除本文件中的中间表
DROP TABLE IF EXISTS temp0;
