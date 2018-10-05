import pandas as pd
from sklearn.utils import shuffle
from sklearn.model_selection import KFold
from sklearn.metrics import roc_auc_score
import time
import xgboost as xgb

train = shuffle(pd.read_csv('atec_anti_fraud_train.csv', sep=','))
train.loc[train['label'] == -1, 'label'] = 1
train_Y = train.pop('label').values
train.drop(['id', 'date'], axis=1, inplace=True)
train_X = train
params = {
    'colsample_bytree': 0.2,
    'learning_rate': 0.01,
    'objective': 'binary:logistic',
    'max_depth': 5,
    'eval_metric': 'auc',
    'silent': 1
}

kf = KFold(n_splits=10, shuffle=True, random_state=666)
for k, (train_index, test_index) in enumerate(kf.split(train_X)):
    start_time = time.time()
    print('k =', k)
    train_data = train_X.iloc[train_index, :]
    test_data = train_X.iloc[test_index, :]
    train_label = [train_Y[ti] for ti in train_index]
    test_label = [train_Y[ti] for ti in test_index]

    dtrain = xgb.DMatrix(train_data, label=train_label)
    dtest = xgb.DMatrix(test_data)
    xgb_model = xgb.train(params, dtrain, 500+100*k)
    test_pred = xgb_model.predict(dtest)
    print('type(test_pred) =', type(test_pred))
    score = roc_auc_score(test_label, test_pred)
    print('k =', k, 'auc_score =', score)
    print('duration =', time.time() - start_time, 's')
