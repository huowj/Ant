import pandas as pd
from sklearn.utils import shuffle
import xgboost as xgb
import random

train = pd.read_csv('/root/ant/atec_anti_fraud_train.csv', sep=',')
test = pd.read_csv('/root/ant/atec_anti_fraud_test_b.csv', sep=',')
train.loc[train['label'] == -1, 'label'] = 1
train = shuffle(train[train['label'] == 1].append(train[train['label'] == 0].sample(frac = 0.3)))
train.drop(['id', 'date'], axis=1, inplace=True)
train = train.fillna(-1)
test_ids = test.pop('id')
test.drop(['date'], axis=1, inplace=True)
test_X = test.fillna(-1)


params = {
    'colsample_bytree': 0.2,
    'learning_rate': 0.01,
    'objective': 'binary:logistic',
    'max_depth': 5,
    'eval_metric': 'auc',
    'silent': 0
}

rs = [42, 0, 71]
preds = []
for i in range(3):
    shuffle(train, random_state=rs[i])
    train_Y = train.pop('label').values
    train_X = train
    dtrain = xgb.DMatrix(train_X, label=train_Y)
    dtest = xgb.DMatrix(test_X)
    xgb_model = xgb.train(params, dtrain, 2000)
    test_pred = xgb_model.predict(dtest)
    preds.append(test_pred)
    train['label'] = train_Y
    print('i =', i, 'done')

rst = pd.DataFrame()
rst['id'] = test_ids
rst['score'] = (preds[0] + preds[1] + preds[2]) / 3
rst[['id', 'score']].to_csv('/root/ant/xgb_ant2_submit_cv_fillna-1.csv', index=False, sep=',')
