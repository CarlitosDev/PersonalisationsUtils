import redshiftSqlAlchemy as rsa
import s3Utils
import pandas as pd
import numpy as np
import os


fName     = 'recommendationsCS.xlsx';
xlsRoot   = '/Users/carlos.aguilar/Documents/Beamly/Fragance Finder/charlotte-FF';
xlsFile   = os.path.join(xlsRoot, fName)
df        = pd.read_excel(xlsFile, sheetname = 'Sheet1')


# Create the connection to RedShift via sqlAlchemy
rs = rsa.RedshiftAlchemy(user='carlos', password='4Beamly4')
    

tableName = 'recom_survey'
df.to_sql(tableName, rs.getDBEngine(), schema='data_science', index = False, if_exists = 'replace')



