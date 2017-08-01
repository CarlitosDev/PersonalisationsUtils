import pandas as pd
import numpy as np
import os
import pickle

metaRoot     = '/Users/carlos.aguilar/Downloads'
jsonFile     = 'the_json2.json'
jsonPath     = os.path.join(metaRoot, jsonFile);
contentsJson = pd.read_json(jsonPath);


metaRoot     = '/Users/carlos.aguilar/Downloads'
xlsFile      = 'the_json.xlsx'
xlsFilePath  = os.path.join(metaRoot, xlsFile);
dataFrameToXLS(contentsJson, xlsFilePath);


import pandas as pd
import numpy as np
import os
import carlosUtils as cu
import bokehUtils as bk
import readMetaDefinitions as rd
import joinMetaDefinitions as jn
import redshiftSqlAlchemy as rsa

metaRoot     = '/Users/carlos.aguilar/Documents/Beamly/Personalisation/adForm data/meta';
jsonFile     = 'browsers.json'
jsonPath     = os.path.join(metaRoot, jsonFile);
browsersMeta = pd.read_json(jsonPath);
browsersMeta.rename(columns={'id': 'BrowserId', 'name': 'BrowserName'}, inplace=True)

# Authenticate with Redshift using your db credentials.
rs = rsa.RedshiftAlchemy(user='carlos', password='4Beamly4')
tableName = 'meta_browsers'
browsersMeta.to_sql(tableName, rs.getDBEngine(), 
    schema='data_science', 
    index = False, 
    if_exists = 'replace', 
    chunksize = 100)
rs.close()




# -------------
jsonFile     = 'geolocations.json'
jsonPath     = os.path.join(metaRoot, jsonFile);
geoMeta      = pd.read_json(jsonPath);
geoMeta.head()


# ------------
df = geoMeta.copy()
#df = df.loc[1:1000, :]

numRows   = len(df);
numChunks = 10
chunkSize = numRows/numChunks

for i in range(0, numChunks):
    a = i
    b = a + chunkSize
    jsonFileTemp = str(i) + jsonFile
    df.loc[a:b, :].to_json(jsonFileTemp, orient='split')




# Pandas to LaTeX
metaRoot     = '/Users/carlos.aguilar/Google Drive/order/Machine Learning Part/results/paperExp1/Express UK';
jsonFile     = 'ExpressUK.xlsx'
jsonPath     = os.path.join(metaRoot, jsonFile);
df           = pd.read_excel(jsonPath, sheetname='forLTX');
latexString = df.to_latex(bold_rows=True);
print(latexString)

thisFile = 'test.tex'


str2File(latexString, thisFile);


# Pandas to LaTeX
metaRoot     = '/Users/carlos.aguilar/Google Drive/order/Machine Learning Part/results/paperExp1/Express UK';
jsonFile     = 'ExpressUK.xlsx'
jsonPath     = os.path.join(metaRoot, jsonFile);
df           = pd.read_excel(jsonPath, sheetname='forLTX');
latexString = df.to_latex(bold_rows=True);
print(latexString)

thisFile = 'test.tex'
f = open('workfile', 'w');
f.write(latexString)
f.close()