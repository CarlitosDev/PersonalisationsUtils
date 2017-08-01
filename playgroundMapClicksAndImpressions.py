import redshiftSqlAlchemy as rsa
import pandas as pd
import pandasql as pdsql
import joinMetaDefinitions as jn
import os
import carlosUtils as cu
import numpy as np

import bokehUtils as bk

import matplotlib.pyplot as plt
from pandas.plotting import parallel_coordinates


# Authenticate with Redshift using your db credentials.
user     = 'carlos_aguilar'
password = 'MdogDI64j6vH90g973'
dbname   = 'adform'
host     = 'adform-ops.c7dxcjhlundm.eu-central-1.redshift.amazonaws.com'

rs = rsa.RedshiftAlchemy(user=user, password=password, 
    database=dbname, host=host)

resultsFolder = '/Users/carlos.aguilar/Documents/Beamly/Personalisation/clickResults';

# JUST ONE CAMPAIGN
saveFigure = True;


numCampaigns = 1000;
sqlQuery = '''SELECT TOP {} * 
FROM adform.impressionsClicksMapped
WHERE campaign_id = 929026
order by yyyy_mm_dd ASC'''.format(numCampaigns)


df = rs.query2DF(sqlQuery)

print(df.keys().tolist())
catVarName = []
varNames   = ['publisher_domain', 'destination_url', 'cookies_enabled', 'devicename', 'countryname']
for varName in varNames:
    currentVarName = varName+'CAT'
    catVarName.append(currentVarName)
    df[currentVarName] = pd.Categorical(df[varName]).codes
    #dataframe.col3, mapping_index = pd.Series(dataframe.col3).factorize()


plt.figure()
pd.plotting.parallel_coordinates(df[catVarName], 'countrynameCAT')
plt.show()
rs.close()




from statsmodels.graphics.mosaicplot import mosaic
mosaic(df, ['publisher_domain', 'destination_url',  'countryname'])
plt.show()


for iVar, iType in df.dtypes.iteritems():
    print(iType)

for a,b in df.dtypes.iteritems():
    print(a)
    print(b)




import os

sqlRoot   = '/Users/carlos.aguilar/Documents/BeamlyRepos/PersonalisationsUtils/sql';
fileName  = 'templateInsertCampaignData.sql'
filePath  = os.path.join(sqlRoot, fileName)

919044
