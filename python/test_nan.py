import pandas as pd
train = pd.read_csv('../atec_anti_fraud_train.csv', sep=',')

# 每列缺失值的个数
col_nans = train.shape[0] - train.count()
col_names = col_nans.index
col_nans = col_nans.values
print('每列缺失值的个数----------------------------------------------')
for i in range(300):
	print(col_names[i], col_nans[i])

# 每行缺失值的个数
row_nans = train.shape[1] - train.count(axis=1)
row_nans = row_nans.values
rn_dict = {}
for rn in row_nans:
	if rn in rn_dict:
		rn_dict[rn] += 1
	else:
		rn_dict[rn] = 1
print('缺失值的个数是？的有？行-------------------------------------')
for i in range(300):
	if i in rn_dict:
		print(i, rn_dict[i])
