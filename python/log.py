#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul  6 07:58:17 2018

@author: hwj
"""

import pandas as pd
import math

result1 = pd.read_csv("/hwj/ant/6_0.5_0.01_200.csv")
result2 = pd.read_csv("/hwj/ant/xgb_ant2_b_depth_6_0.5_100.csv")

a = result1['score'].apply(lambda x: math.log(x/(1-x)))
b = result2['score'].apply(lambda x: math.log(x/(1-x)))
c = 0.5*a+0.5*b
z = c.apply(lambda x: 1/(1+math.exp(-x)))
print(z.max())
print(z.min())
print(z.mean())

res = result1.id
res = pd.concat([res, z], axis=1)
res.to_csv('/hwj/ant/result_merge.csv', index=0)