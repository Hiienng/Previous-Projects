import pandas as pd

df_g2_riskpremium = pd.read_csv('WorldBank-riskpremium/API_FR.INR.RISK.csv', skiprows=4)
df_g2_persremi =  pd.read_csv('WorldBank-personalremittances/API_BX.TRF.PWKR.CD.DT_DS2.csv',skiprows=4)
df_g2_lendingir =  pd.read_csv('WorldBank-lendingir/API_FR.INR.LEND_DS2.csv',skiprows=4)
df_g2_depositors =  pd.read_csv('WorldBank-depositors/API_FB.CBK.BRWR.P3_DS2.csv',skiprows=4)


df_g1_riskpremium = pd.read_csv('WorldBank-riskpremium/Metadata_Indicator_API_FR.INR.RISK.csv')
df_g1_persremi =  pd.read_csv('WorldBank-personalremittances/Metadata_Indicator_API_BX.TRF.PWKR.CD.DT_DS2.csv')
df_g1_lendingir =  pd.read_csv('WorldBank-lendingir/Metadata_Indicator_API_FR.INR.LEND_DS2.csv')
df_g1_depositors =  pd.read_csv('WorldBank-depositors/Metadata_Indicator_API_FB.CBK.BRWR.P3_DS2.csv')
df_g1_indicators = pd.concat([df_g1_riskpremium, df_g1_persremi,df_g1_lendingir,df_g1_depositors], ignore_index=True)

