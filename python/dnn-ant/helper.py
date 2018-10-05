import pandas as pd
import random
import numpy as np


def getOriginalData():
	data = pd.read_csv('/root/ant/atec_anti_fraud_train.csv', sep=',').fillna(-1)
	
	train = data[data['date'] <= 20171025]
	train_0 = train[train.label == 0]
	train_1 = train[train.label != 0]
	train_0.drop(['id', 'date', 'label'], axis=1, inplace=True)
	train_1.drop(['id', 'date', 'label'], axis=1, inplace=True)
	train_0 = train_0.values
	train_1 = train_1.values
	
	valid = data[data['date'] >  20171025]
	valid.drop(['id', 'date'], axis=1, inplace=True)
	
	return (train_0, train_1, len(train_0), len(train_1), valid.values)


def getBatchData(train_0, train_1, len_0, len_1, valid, ratio=0.5, batch_size=128*100):
	while True:
		mlp_X, mlp_Y = [], []
		for i in range(batch_size):
			if random.random() > ratio: # 0-0
				lucky_dogs = random.sample(range(len_0), 2)
				mlp_X.append(getVector(train_0[lucky_dogs[0]], train_0[lucky_dogs[1]]))
				mlp_Y.append(0)
			else: # 0-1
				index0 = random.randint(0, len_0-1)
				index1 = random.randint(0, len_1-1)
				mlp_X.append(getVector(train_0[index0], train_1[index1]))
				mlp_Y.append(1)
		yield (np.array(mlp_X), np.array(mlp_Y))


def getVector(record1, record2):
	v = []
	for i in range(297):
		if i not in [81, 82, 83, 84, 85]: # f82, f83, f84, f85, f86
			v.append(1 if record1[i] == record2[i] else 0) # 暂时不考虑缺失值的情况
		else:
			zone_len = [100, 200, 400, 150, 150]
			r = (record1[i] / 10000 - record2[i] / 10000) / zone_len[i-81]
			r = -1.0 if r < -1.0 else r
			r = 1.0 if r > 1.0 else r
			v.append(r)
	return v
