import pandas as pd
from sklearn.utils import shuffle
import xgboost as xgb

train = pd.read_csv('atec_anti_fraud_train.csv', sep=',')
test = pd.read_csv('atec_anti_fraud_test_a.csv', sep=',')
train = shuffle(train[train.label != -1])
train_Y = train.pop('label').values
train.drop(['id', 'date'], axis=1, inplace=True)
train_X = train
test_ids = test.pop('id')
test.drop(['date'], axis=1, inplace=True)
test_X = test

params = {
    'colsample_bytree': 0.1,
    'learning_rate': 0.02,
    'objective': 'binary:logistic',
    'max_depth': 5,
    'eval_metric': 'auc',
    'silent': 1
}

dtrain = xgb.DMatrix(train_X, label=train_Y)
dtest = xgb.DMatrix(test_X)

xgb_model = xgb.train(params, dtrain, 1000)
test_pred = xgb_model.predict(dtest)
print(len(test_pred))
print(test_pred[0])

rst = pd.DataFrame()
rst['id'] = test_ids
rst['score'] = test_pred
rst[['id', 'score']].to_csv('./xgb_ant_submit.csv', index=False, sep=',')
