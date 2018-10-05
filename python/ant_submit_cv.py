import pandas as pd
from sklearn.utils import shuffle
import xgboost as xgb

train = pd.read_csv('/hwj/ant/atec_anti_fraud_train.csv', sep=',')
test = pd.read_csv('/hwj/ant/atec_anti_fraud_test_b.csv', sep=',')
train = shuffle(train[train.label != -1])
train.drop(['id', 'date'], axis=1, inplace=True)
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

rs = [42, 666, 2018]
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
rst[['id', 'score']].to_csv('./xgb_ant_submit_cv.csv', index=False, sep=',')
