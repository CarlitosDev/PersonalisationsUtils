"""

    This file reads the meta definitions created with uploadMetasToRedShift.py
    and parses the campaignsname column to get the information about the campaign

    Before uploading the table to Redshift, the SQL script extendCampaignInformation.sql
    must be run to clear all the types and incorrect entries so the regEx works

    The merging of the campaign tables is done in mapImpressionsClicksAndTrackingPoints.sql

"""


import redshiftSqlAlchemy as rsa
import pandas as pd
import pandasql as pdsql
import joinMetaDefinitions as jn
import os
import carlosUtils as cu
import re
import bokehUtils as bk


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


sqlQuery = '''select *
from adform.meta_campaigns
where (len(campaignsname)- len(replace(campaignsname, '_', ''))) >= 8
'''

df = rs.query2DF(sqlQuery)


cl1 = lambda x: str.replace(x, "___","_none_none_")
cl2 = lambda x: str.replace(x, "__" ,"_none_")
df.campaignsname = df.campaignsname.apply(cl1)
df.campaignsname = df.campaignsname.apply(cl2)

varName      = 'campaignsname'
regExPattern = '^([^_]+)+[_.]+([^+]+)+[+.]+([^_]+)+[_.]+([^_]+)+[_.]+([^_]+)+[_.]+([^_]+)+[_.]+([^_]+)+[_.]+([^_]+)+[_.]+([^_]+)+[_.]+([^_]+)$'

colNames = ['Market','Year','Quarter','agency','MasterBrand', \
    'Brand+CampaignEvent','CotyCampaignCode','BionicCampaignId', \
    'CampaignObjective','ClientName']

campaignsDF = cu.getDFfromRegEx(df, varName, regExPattern, colNames)



campaignsDF[varName] = df[varName]
xlsFile = 'campaignExtended.xlsx'
#cu.dataFrameToXLS(campaignsDF, xlsFile);

tableName = 'meta_ext_campaign'
campaignsDF.to_sql(tableName, rs.getDBEngine(), 
    schema='adform', 
    index = False, 
    if_exists = 'replace', 
    chunksize = 100)
print('Table {} uploaded...'.format(tableName))


rs.close()