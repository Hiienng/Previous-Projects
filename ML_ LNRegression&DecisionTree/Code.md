# Project 2
- Method: LinearRegression + RandomForest
- Reference to: 
- Data source: Github

## 1. Data preparation and loading

    import pandas as pd
    df = pd.read_csv('https://raw.githubusercontent.com/dataprofessor/data/master/delaney_solubility_with_descriptors.csv')

## 2. Data splitting

    y = df['logS']
    x = df.drop('logS', axis=1)

    from sklearn.model_selection import train_test_split
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size = 0.2, random_state =100)

## 3 Traning model 
### 3.1 Linear Regression
Apply the model

    from sklearn.linear_model import LinearRegression

    lr = LinearRegression()
    lr.fit(x_train, y_train)

    y_lr_train_pred = lr.predict(x_train)
    y_lr_test_pred = lr.predict(x_test)

Evaluation

    from sklearn.metrics import mean_squared_error, r2_score

    lr_train_mse = mean_squared_error(y_train, y_lr_train_pred)
    lr_train_r2 = r2_score(y_train, y_lr_train_pred)

    lr_test_mse = mean_squared_error(y_test, y_lr_test_pred)
    lr_test_r2 = r2_score(y_test, y_lr_test_pred)
    
### 3.1 Random Forest
Apply the model

    from sklearn.ensemble import RandomForestRegressor

    rf = RandomForestRegressor(max_depth=2, random_state=100)
    rf.fit(x_train, y_train)

    y_rf_train = rf.predict(x_train)
    y_rf_test = rf.predict(x_test)

Evaluation 

    from sklearn.metrics import mean_squared_error, r2_score

    rf_train_sme = mean_squared_error(y_train, y_rf_train)
    rf_train_r2 = r2_score(y_train, y_rf_train)

    rf_test_sme = mean_squared_error(y_test, y_rf_test)
    rf_test_r2 = r2_score(y_test, y_rf_test)
    
## 4. A look an 2 approaches

    rf_result = pd.DataFrame(['Random Forest', rf_train_sme, rf_train_r2,rf_test_sme,rf_test_r2]).transpose()
    rf_result.columns = ['Method','MSE Train','R2 Train','MSE Test','R2 Test']
    Model_comparison = pd.concat([lr_result,rf_result], axis = 0).reset_index(drop=True)
