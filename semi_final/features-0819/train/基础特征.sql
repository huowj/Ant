-- 3个特征表：(11+30+29=70)
-- (11) wen_train_one_hot
-- (30) wen_train_ic_features
-- (29) wen_train_discrete_order(需要先执行字段排序.odps)

-- 共11个特征（对部分离散特征进行独热编码）
DROP TABLE IF EXISTS wen_train_one_hot;
CREATE TABLE IF NOT EXISTS wen_train_one_hot AS
SELECT event_id, 
    IF(is_one_people = 'ce8d628e0da6ca9872afb848fe4f0c87b45ee8c45d7b2a1cf3c0f0398e99af31', 1, 0) 
    AS is_one_people_encode,
    IF(mobile_oper_platform = '4c4972bc9d20e1f18399978430eea92b6ec5c2f522afdb3d25a94b05eaffadaf', 1, 0)
    AS mobile_oper_platform_oh1,
    IF(mobile_oper_platform = '917fa5899d1635954c6e058c3b3f890cd55ece90b78ba9410b90024046e1f883', 1, 0)
    AS mobile_oper_platform_oh2,
    IF(mobile_oper_platform = '56ea4cf2c3255f17ce746d73bb4867a9cf6fcd5fe89bb6fef0ef13d6b7aef089', 1, 0)
    AS mobile_oper_platform_oh3,
    IF(mobile_oper_platform = '0fc43b62c9f2454bc54d9e6df22afb9b26d373702ab2c6e528df94f3e8d9397d', 1, 0)
    AS operation_channel_oh1,
    IF(operation_channel = '9a6fdfa6ccb3eeaf1b0825d322f22d024d7a8f112f286849ea1e7431e7a94da0', 1, 0)
    AS operation_channel_oh2,
    IF(operation_channel = 'b99f224a5cfebb921303f9e2d01ab0c2991e9e2742bfbcb69194cee4620f9a10', 1, 0)
    AS operation_channel_oh3,
    IF(operation_channel = '5c73c7a4287cc7f93b92fcdc90536be9cb65c7c50fbd69df05d13396fbe8d68c', 1, 0)
    AS operation_channel_oh4,
    IF(is_peer_pay is NULL, 1, 0) AS is_peer_pay_null,
    IF(is_peer_pay = '85d8c6ac08d5eb8a634919182f6699d1889df0364531d1425a864a00af5571ba', 1, 0)
    AS is_peer_pay_oh1,
    IF(is_peer_pay = 'd57835fe5b36ac9cf1f4e905462e51a0cb58bb9d7e167cc70c44834392bf2138', 1, 0)
    AS is_peer_pay_oh2
FROM atec_1000w_ins_data;


-- 同一条记录省市信息比较，共计30个特征
DROP TABLE IF EXISTS wen_train_ic_features;
CREATE TABLE IF NOT EXISTS wen_train_ic_features AS
SELECT event_id,
    IF(ip_prov = cert_prov, 1, 0) AS ic_prov,
    IF(ip_prov = card_bin_prov, 1, 0) AS icb_prov,
    IF(ip_prov = card_mobile_prov, 1, 0) AS icm_prov,
    IF(ip_prov = card_cert_prov, 1, 0) AS icc_prov,
    IF(cert_prov = card_bin_prov, 1, 0) AS ccb_prov,
    IF(cert_prov = card_mobile_prov, 1, 0) AS ccm_prov,
    IF(cert_prov = card_cert_prov, 1, 0) AS ccc_prov,
    IF(card_bin_prov = card_mobile_prov, 1, 0) AS cbcm_prov,
    IF(card_bin_prov = card_cert_prov, 1, 0) AS cbcc_prov,
    IF(card_mobile_prov = card_cert_prov, 1, 0) AS cmcc_prov,
    IF(province = ip_prov, 1, 0) AS pi_prov,
    IF(province = cert_prov, 1, 0) AS pc_prov,
    IF(province = card_bin_prov, 1, 0) AS pcb_prov,
    IF(province = card_mobile_prov, 1, 0) AS pcm_prov,
    IF(province = card_cert_prov, 1, 0) AS pcc_prov,
    IF(ip_city = cert_city, 1, 0) AS ic_city,
    IF(ip_city = card_bin_city, 1, 0) AS icb_city,
    IF(ip_city = card_mobile_city, 1, 0) AS icm_city,
    IF(ip_city = card_cert_city, 1, 0) AS icc_city,
    IF(cert_city = card_bin_city, 1, 0) AS ccb_city,
    IF(cert_city = card_mobile_city, 1, 0) AS ccm_city,
    IF(cert_city = card_cert_city, 1, 0) AS ccc_city,
    IF(card_bin_city = card_mobile_city, 1, 0) AS cbcm_city,
    IF(card_bin_city = card_cert_city, 1, 0) AS cbcc_city,
    IF(card_mobile_city = card_cert_city, 1, 0) AS cmcc_city,
    IF(city = ip_city, 1, 0) AS pi_city,
    IF(city = cert_city, 1, 0) AS pc_city,
    IF(city = card_bin_city, 1, 0) AS pcb_city,
    IF(city = card_mobile_city, 1, 0) AS pcm_city,
    IF(city = card_cert_city, 1, 0) AS pcc_city
FROM atec_1000w_ins_data;

-- join字段排序.odps中的29个表，得到29个特征
DROP TABLE IF EXISTS wen_train_discrete_order;
CREATE TABLE IF NOT EXISTS wen_train_discrete_order AS
SELECT event_id, 
    client_ip_row_number,
    network_row_number,
    device_sign_row_number,
    info_1_row_number,
    info_2_row_number,
    ip_prov_row_number,
    ip_city_row_number,
    cert_prov_row_number,
    cert_city_row_number,
    card_bin_prov_row_number,
    card_bin_city_row_number,
    card_mobile_prov_row_number,
    card_mobile_city_row_number,
    card_cert_prov_row_number,
    card_cert_city_row_number,
    is_one_people_row_number,
    mobile_oper_platform_row_number,
    operation_channel_row_number,
    pay_scene_row_number,
    amt_row_number,
    card_cert_no_row_number,
    income_card_no_row_number,
    income_card_cert_no_row_number,
    income_card_mobile_row_number,
    income_card_bank_code_row_number,
    province_row_number,
    city_row_number,
    is_peer_pay_row_number,
    version_row_number
FROM atec_1000w_ins_data t 
LEFT JOIN huo_client_ip_row ON t.client_ip = huo_client_ip_row.client_ip
LEFT JOIN huo_network_row ON t.network = huo_network_row.network
LEFT JOIN huo_device_sign_row ON t.device_sign = huo_device_sign_row.device_sign
LEFT JOIN huo_info_1_row ON t.info_1 = huo_info_1_row.info_1
LEFT JOIN huo_info_2_row ON t.info_2 = huo_info_2_row.info_2
LEFT JOIN huo_ip_prov_row ON t.ip_prov = huo_ip_prov_row.ip_prov
LEFT JOIN huo_ip_city_row ON t.ip_city = huo_ip_city_row.ip_city
LEFT JOIN huo_cert_prov_row ON t.cert_prov = huo_cert_prov_row.cert_prov
LEFT JOIN huo_cert_city_row ON t.cert_city = huo_cert_city_row.cert_city
LEFT JOIN huo_card_bin_prov_row ON t.card_bin_prov = huo_card_bin_prov_row.card_bin_prov
LEFT JOIN huo_card_bin_city_row ON t.card_bin_city = huo_card_bin_city_row.card_bin_city
LEFT JOIN huo_card_mobile_prov_row ON t.card_mobile_prov = huo_card_mobile_prov_row.card_mobile_prov
LEFT JOIN huo_card_mobile_city_row ON t.card_mobile_city = huo_card_mobile_city_row.card_mobile_city
LEFT JOIN huo_card_cert_prov_row ON t.card_cert_prov = huo_card_cert_prov_row.card_cert_prov
LEFT JOIN huo_card_cert_city_row ON t.card_cert_city = huo_card_cert_city_row.card_cert_city
LEFT JOIN huo_is_one_people_row ON t.is_one_people = huo_is_one_people_row.is_one_people
LEFT JOIN huo_mobile_oper_platform_row ON t.mobile_oper_platform = huo_mobile_oper_platform_row.mobile_oper_platform
LEFT JOIN huo_operation_channel_row ON t.operation_channel = huo_operation_channel_row.operation_channel
LEFT JOIN huo_pay_scene_row ON t.pay_scene = huo_pay_scene_row.pay_scene
LEFT JOIN huo_amt_row ON t.amt = huo_amt_row.amt
LEFT JOIN huo_card_cert_no_row ON t.card_cert_no = huo_card_cert_no_row.card_cert_no
LEFT JOIN huo_income_card_no_row ON t.income_card_no = huo_income_card_no_row.income_card_no
LEFT JOIN huo_income_card_cert_no_row ON t.income_card_cert_no = huo_income_card_cert_no_row.income_card_cert_no
LEFT JOIN huo_income_card_mobile_row ON t.income_card_mobile = huo_income_card_mobile_row.income_card_mobile
LEFT JOIN huo_income_card_bank_code_row ON t.income_card_bank_code = huo_income_card_bank_code_row.income_card_bank_code
LEFT JOIN huo_province_row ON t.province = huo_province_row.province
LEFT JOIN huo_city_row ON t.city = huo_city_row.city
LEFT JOIN huo_is_peer_pay_row ON t.is_peer_pay = huo_is_peer_pay_row.is_peer_pay
LEFT JOIN huo_version_row ON t.version = huo_version_row.version;
