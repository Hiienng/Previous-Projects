# Project 1 - House Pricing Evaluation

- Method: DecisionTree + RandomForest
- Reference to: https://www.youtube.com/watch?v=wujyckteVYM&t=213s
- Data source: Kaggle.com

## 1. Preprocessing data

    import pandas as pd
    import numpy as np

    df = pd.read_csv("train.csv", sep = ",")
    needed_features =['LotArea','YearBuilt','1stFlrSF','2ndFlrSF','FullBath','BedroomAbvGr','TotRmsAbvGrd']
## 2. Splitting Dataset

    x = df[needed_features]
    y = df['SalePrice']

    from sklearn.model_selection import train_test_split
    x_train, x_test, y_train, y_test = train_test_split(x,y,test_size = 0.2, random_state = 0)
## 3. Training Model

    from sklearn.tree import DecisionTreeRegressor
    dt_model = DecisionTreeRegressor(random_state=1)

    dt_model.fit(x_train,y_train)

    y_train_pre = dt_model.predict(x_train.head())
    y_test_pre = dt_model.predict(x_test.head())

## 4. Model Evaluation

    from sklearn.metrics import mean_squared_error, r2_score

    lr_train_mse = mean_squared_error(y_train, y_train_pre)
    lr_train_r2 = r2_score(y_train, y_train_pre)

    lr_test_mse = mean_squared_error(y_test, y_test_pre)
    lr_test_r2 = r2_score(y_test, y_test_pre)
