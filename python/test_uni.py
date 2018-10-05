import pandas as pd
train = pd.read_csv('../atec_anti_fraud_test_b.csv', sep=',')
# 每个特征的可能取值的个数
for i in range(1, 297+1):
	fu = pd.unique(train['f' + str(i)])
	print(i, len(list(fu)))

# 82, 83, 84, 85, 86
for i in range(82, 86+1):
	print(i, train['f' + str(i)].max(), train['f' + str(i)].min())
print('')
print('date', train['date'].max(), train['date'].min())
