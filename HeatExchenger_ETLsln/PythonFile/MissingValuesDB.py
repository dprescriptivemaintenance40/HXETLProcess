import sys
import pandas as pd
import pyodbc as odbc


conn = odbc.connect('Driver=SQL SERVER;Server=DESKTOP-6EBQLBP;Database=DPM;Integrated Security=True')

cursor = conn.cursor()
batchId=int(sys.argv[1])
print(batchId)
# batchId=2
# AssetName="CentrifugalCompressor"

#ScrewParamter
query='''SELECT * FROM HXCleaningTables WHERE HXId={}'''
MissVal = pd.read_sql(query.format(batchId),conn, parse_dates=['Date'])
print(MissVal)
# MissVal.drop_duplicates(subset=['Date'],inplace=True)
# Step 3 :- Generatte dates from min and max values
date_range = pd.DataFrame({'Date': pd.date_range(min(MissVal['Date']),max(MissVal['Date']), freq='D')})
# Step 4 :- Make left join with Missing values.
c = pd.merge(pd.DataFrame(date_range), pd.DataFrame(MissVal), 
              left_on=['Date'],
              right_on= ['Date'], how='left')
for col in c.columns:
    if not col=="Date":
      if not col=="Id":
        if not col=="HXId":
          c[col].interpolate(method ='linear',inplace=True)
for i, row in c.iterrows():
  if pd.isnull(row['HXId']):
        row['HXId']=batch
  else:
        row['HXId']=int(row['HXId'])
        batch=int(row['HXId'])
  SQLCommand = "INSERT INTO HXProcessedTables(HXId,Date,TT1,TT2,TS1,TS2,FT1,FT2,PT1,PT2,PS1,PS2,Mass,SpecificHeat,HTC,HeatEnergy,LMTD,Area) VALUES('" + str(row['HXId']) + "','" + str(row['Date']) + "','" + str(row['TT1']) + "','" + str(row['TT2']) + "','" + str(row['TS1']) + "','" + str(row['TS2']) + "','" + str(row['FT1']) + "','" + str(row['FT2']) + "','" + str(row['PT1']) + "','" + str(row['PT2']) + "','" + str(row['PS1']) + "','" + str(row['PS2']) + "','" + str(row['Mass']) + "','" + str(row['SpecificHeat']) + "','" + str(row['HTC']) + "','" + str(row['HeatEnergy']) + "','" + str(row['LMTD']) + "','" + str(row['Area']) + "')"
  cursor.execute(SQLCommand)     
conn.commit()


