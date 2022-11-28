from typing import final
import sys
import pyodbc as odbc
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.ar_model import AutoReg
import numpy as np
from datetime import date, datetime
bigdata = pd.DataFrame()

conn = odbc.connect('Driver=SQL SERVER;Server=DESKTOP-CGG65T8;Database=DPM;Integrated Security=True')

cursor = conn.cursor()
# batchId=1
# AssetName="CentrifugalCompressor"
batchId=int(sys.argv[1])

query='''SELECT * FROM HXProcessedTables WHERE HXId={}'''
series = pd.read_sql(query.format(batchId),conn, parse_dates=['Date'])
startdate=min(series["Date"])
series.set_index('Date')
    # Step 3 :- 
series.index=pd.to_datetime(series.index, unit='D',origin=startdate)
    # Step 4 :- Season decomponse it in to observer , seasonal , trend
for col in series.columns:
  print("Col name" + col)
  if not col=="Date":
    if not col=="Id":
      if not col=="HXId":
                result=seasonal_decompose(series[col], model='additive', period=365,extrapolate_trend='freq')
                resultFinal = pd.concat([result.observed, result.seasonal,result.trend,result.resid],axis=1)
                resultFinal.index.name  = col+"Date"
                # Extract the day and month wise Seasonal and residal
                # Step 5 :- Added the totalunknow=seasonal + residual
                #resultFinal[col+'resses'] =result.seasonal+result.resid
                resultFinal[col+'resses'] =result.seasonal+result.resid
                resultFinal[col+'datemonth']  = resultFinal.index.strftime("%m/%d")
                datemonth = pd.DataFrame(columns = [col+'datemonth',col+'average'])
                datemonth[col+'datemonth'] = resultFinal[col+'datemonth']
                # Step 6 :0 datemonth is having date and month with averag
                dtmean=resultFinal.groupby([col+'datemonth'])[col+'resses'].mean()
                #print(dtmean)
                #plt.show()
                # Predict using auto regression
                Training = resultFinal[col].copy()
                model = AutoReg(Training, lags=3)
                model_fit = model.fit()
                predictedValue = model_fit.predict(0, len(resultFinal)+400)

                #predictedValue["finalvalue"]=0
                #predictedValue['datemonth']  = predictedValue.iloc[:, [0]].strftime("%m/%d")
                panda1 = pd.DataFrame({col+'Date':predictedValue.index,col+ 'ValuePredicted':predictedValue.values})
                panda1[col+"datemonth"]= panda1[col+"Date"].dt.strftime("%m/%d")

                finalDf = pd.merge(pd.DataFrame(panda1), pd.DataFrame(dtmean), left_on=[col+'datemonth'], 
                            right_on= [col+'datemonth'], how='left')
                # value predited from autoregression plus unknows
                # Add the auression + Seasonal + Resid
                finalDf[col+"New"] = finalDf[col+"ValuePredicted"] + finalDf[col+"resses"]
            
                finalDf.set_index(col+'Date', inplace=True)
                bigdata[col]=finalDf[col+"New"]
for i, row in bigdata.iterrows():
      if pd.isnull(row['TT1']) and pd.isnull(row['TT2']) and pd.isnull(row['TS1']) and pd.isnull(row['TS2']) and pd.isnull(row['FT1']) and pd.isnull(row['FT2']) and pd.isnull(row['PT1']) and pd.isnull(row['PT2']) and pd.isnull(row['PS1']) and pd.isnull(row['PS2']) and pd.isnull(row['Mass']) and pd.isnull(row['SpecificHeat']) and pd.isnull(row['HTC']) and pd.isnull(row['HeatEnergy']) and pd.isnull(row['LMTD']) and pd.isnull(row['Area']) :
         row['TT1']=0
         row['TT2']=0
         row['TS1']=0
         row['TS2']=0
         row['FT1']=0
         row['FT2']=0
         row['PT1']=0
         row['PT2']=0
         row['PS1']=0
         row['PS2']=0
         row['Mass']=0
         row['SpecificHeat']=0
         row['HTC']=0
         row['HeatEnergy']=0
         row['LMTD']=0
         row['Area']=0
      SQLCommand = "INSERT INTO HXPredictedTables(HXId,Date,TT1,TT2,TS1,TS2,FT1,FT2,PT1,PT2,PS1,PS2,Mass,SpecificHeat,HTC,HeatEnergy,LMTD,Area) VALUES('" + str(batchId) + "','" + str(i) + "','" + str(row['TT1']) + "','" + str(row['TT2']) + "','" + str(row['TS1']) + "','" + str(row['TS2']) + "','" + str(row['FT1']) + "','" + str(row['FT2']) + "','" + str(row['PT1']) + "','" + str(row['PT2']) + "','" + str(row['PS1']) + "','" + str(row['PS2']) + "','" + str(row['Mass']) + "','" + str(row['SpecificHeat']) + "','" + str(row['HTC']) + "','" + str(row['HeatEnergy']) + "','" + str(row['LMTD']) + "','" + str(row['Area']) + "')"
      cursor.execute(SQLCommand)
conn.commit()

