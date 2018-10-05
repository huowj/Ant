# -*- coding: utf-8 -*-
import pandas as pd
import lightgbm as lgb
from sklearn import metrics
import numpy as np
from sklearn.model_selection import GridSearchCV

# 导入训练集
reader = pd.read_csv("/Users/xueli/Documents/atec/atec_anti_fraud_train.csv", iterator=True)
try:
    df = reader.get_chunk(100000000)
except StopIteration:
    print("Iteration is stopped.")
df1 = df.fillna(0)

dates = np.array(list(set(df1['date'].values)))
dates = dates[np.argsort(dates)]
# train_rank = dates[0:30].reshape((7,6))


v_dtrain = df1[df1["label"] != -1][df1['date'].isin(dates[0:30])]
v_dtest = df1[df1['date'].isin(dates[30:62])]
v_dtrain_t = df1[df1['date'].isin(dates[0:30])]

target = "label"
predictors = [x for x in df1.columns if x not in ["id", "label", "date"]]
d_train = lgb.Dataset(v_dtrain[predictors], label=v_dtrain[target])
d_test = lgb.Dataset(v_dtest[predictors], label=v_dtest[target])

####得分函数#########
from sklearn.metrics import roc_curve


def score(y, pred):
    fpr, tpr, thresholds = roc_curve(y, pred, pos_label=1)
    score = 0.4 * tpr[np.where(fpr > 0.001)[0][0] - 1] + 0.3 * tpr[np.where(fpr > 0.005)[0][0] - 1] + 0.3 * tpr[
        np.where(fpr > 0.01)[0][0] - 1]
    return score


#############

params = {"boosting_type": 'gbdt', "objective": 'binary', "max_depth": 6, "learning_rate": 0.05,
          "num_iterations": 200, "min_child_weight": 500, "bagging_fraction": 0.6,
          "feature_fraction": 0.6, "scale_pos_weight": 4,
          "num_leaves": 10, "seed": 27, "reg_alpha": 0.005,
          "n_estimators": 300
          }

params1 = {"boosting_type": 'gbdt', "objective": 'binary', "max_depth": 10, "learning_rate": 0.05,
           "num_iterations": 200, "min_child_weight": 100, "bagging_fraction": 1,
           "feature_fraction": 0.2, "scale_pos_weight": 1,
           "num_leaves": 10, "seed": 27, "reg_alpha": 0.1,
           "n_estimators": 300
           }

model2 = lgb.train(params, d_train)

###单模型评分##################
pred_train = model2.predict(v_dtrain[predictors])
print(score(v_dtrain[target], pred_train))

pred_train_t = model2.predict(v_dtrain_t[predictors])
print(score(v_dtrain_t[target], pred_train_t))

pred_test = model2.predict(v_dtest[predictors])
print(score(v_dtest[target], pred_test))