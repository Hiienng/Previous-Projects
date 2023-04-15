# Project 3 - Net interest income prediction
- Description: Base on the Bank's nature, it's history performance and the previous year Balance Sheet, this year planning disbursement (in this project, ENR is used instead), this project will provide the estimation of Net interest income (NII) of the year.
- Project's value: This project helps BOM in planning the Targetted Disbursement volumes (here ENR instead) of the year so as to achieve the NII, as well as Profit
- Data source: Public Financial Statements of the banks


    import pandas as pd

    df = pd.read_csv('Raw.csv',sep= ',').dropna()
    df.columns = df.columns.str.strip()
    df

    df.columns = df.columns.str.strip()
    df = df.replace({'\$': ''}, regex=True)
    df

    cols = ['Cur_TA_ENR', 'Pre_Total_Asset', 'Pre_TA_ENR', 'Pre_Liability', 'Pre_Equity', 'Pre_Retain_earning', 'NII']
    df[cols] = df[cols].astype(float)
    
    y = df['NII']
    x = df.drop(['NII','Bank'], axis=1)

    from sklearn.model_selection import train_test_split
    x_train, x_test, y_train , y_test = train_test_split(x,y, train_size=0.8, random_state=0)

    from sklearn.tree import DecisionTreeRegressor
    dt_model = DecisionTreeRegressor(random_state=1)

    dt_model.fit(x_train,y_train)
    y_train_pre = dt_model.predict(x_train)
    y_test_pre = dt_model.predict(x_test)
    
    from sklearn.metrics import mean_squared_error, r2_score

    lr_train_mse = mean_squared_error(y_train, y_train_pre)
    lr_train_r2 = r2_score(y_train, y_train_pre)

    lr_test_mse = mean_squared_error(y_test, y_test_pre)
    lr_test_r2 = r2_score(y_test, y_test_pre)
