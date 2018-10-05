#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 26 11:06:33 2018

@author: hwj
"""


import pandas as pd

train = pd.read_csv("/hwj/ant/atec_anti_fraud_train.csv")
test = pd.read_csv("/hwj/ant/atec_anti_fraud_test_a.csv")

train_columns = train.columns.tolist()
# id label date f1-f297
test_columns = test.columns.tolist()
# id date f1-f297

"""
train.date
20170905 - 20171105

test.date
20180105 - 20180205

train.label
-1      4725
 0    977884
 1     12122
"""



for i in range(30):
    data = train.iloc[:,(10*i) : ((10*i) + 10)]
    data.to_csv("/hwj/ant/train_v" + str(10*i) + "-v" + str(10*i + 10) + ".csv", index = 0)