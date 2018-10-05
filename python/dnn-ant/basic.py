import helper
(train_0, train_1, len_0, len_1, valid) = helper.getOriginalData()
yield_data = helper.getBatchData(train_0, train_1, len_0, len_1, valid)


from keras.models import Sequential
from keras.layers import Dense, Dropout
from keras.optimizers import SGD
def build_model(n=297):
    model = Sequential()
    model.add(Dense(units=200, input_dim=n, activation='relu'))
    model.add(Dropout(0.5))
    model.add(Dense(units=100, activation='relu'))
    model.add(Dropout(0.5))
    model.add(Dense(units=50, activation='relu'))
    model.add(Dropout(0.5))
    model.add(Dense(units=20, activation='relu'))
    model.add(Dense(units=5, activation='relu'))
    model.add(Dense(units=1, activation='sigmoid'))
	
    model.compile(loss='binary_crossentropy',
                  optimizer=SGD(lr=0.01, decay=1e-6, momentum=0.9),
                  metrics=['accuracy'])
    return model

model = build_model()
i = 0
for (mlp_X, mlp_Y) in yield_data:
	model.fit(mlp_X, mlp_Y, epochs=1, batch_size=128)
	print(i)
	i += 1
