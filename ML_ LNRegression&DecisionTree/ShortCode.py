import pandas as pd
import numpy as np

# 1. Preprocess: Import dataset
df = pd.read_csv("C:\Users\Hien\OneDrive\Documents\Machine Learning\Project 1_HousingPricingEvaluation_DecisionTree\Data\train.csv", sep = ",")

# 2. Feature Selection
needed_features =['LotArea','YearBuilt','1stFlrSF','2ndFlrSF','FullBath','BedroomAbvGr','TotRmsAbvGrd']

# 3. Slitting 
x = df[needed_features]
y = df['SalePrice']

from sklearn.model_selection import train_test_split
x_train, x_test, y_train, y_test = train_test_split(x,y,test_size = 0.2, random_state = 0)

# # 4. Training model: Decision Tree Regressor
# from sklearn.tree import DecisionTreeRegressor

# dt_model = DecisionTreeRegressor(random_state=1)
# dt_model.fit(x_train,y_train)

# y_train_pre = dt_model.predict(x_train.head())
# y_test_pre = dt_model.predict(x_test.head())

# #Overview
# pd.DataFrame({'y_test' : y_test.head(), 'y_test_pre': y_test_pre})

# 4' Training model: Random Forest Regression
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor

rf_model = RandomForestRegressor(random_state=1)
rf_model.fit(x_train, y_train)

y_train_pre_rf = rf_model.predict(x_train.head())
y_test_pre_rf = rf_model.predict(x_test.head())
#Overview
pd.DataFrame({'x_test' :x_test.head(), 'y_test_pre_rf' : y_test_pre_rf})
