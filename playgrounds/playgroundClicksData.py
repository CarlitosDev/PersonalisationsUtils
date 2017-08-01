import redshiftSqlAlchemy as rsa
import pandas as pd
import pandasql as pdsql
import joinMetaDefinitions as jn
import os
import carlosUtils as cu

import bokehUtils as bk


# Authenticate with Redshift using your db credentials.
user     = 'carlos_aguilar'
password = 'MdogDI64j6vH90g973'
dbname   = 'adform'
host     = 'adform-ops.c7dxcjhlundm.eu-central-1.redshift.amazonaws.com'

rs = rsa.RedshiftAlchemy(user=user, password=password, 
    database=dbname, host=host)



# JUST ONE CAMPAIGN

currentCampaignId = 922742

sqlQuery = '''SELECT
campaign_id,
clientsname,
countryname,
devicename,
sum(numRecords) as totalClicks
from adform.clickExtended
where campaign_id = {}
group by 1,2,3,4
order by 1,2,totalclicks DESC'''.format(currentCampaignId)

df = rs.query2DF(sqlQuery)
df.head()
totalCampaignClicks = df['totalclicks'].sum();
df['perct'] = 100.0*df['totalclicks']/totalCampaignClicks;
# cutoff at 95%
idx95p = df['perct'].cumsum() < 96.0

df95p = df.loc[idx95p, :]

title = df.clientsname[0] + ' (id ' + str(currentCampaignId) + ') clicks: ' + str(totalCampaignClicks)
labels = ['devicename','countryname']
values='totalclicks'
hoverText ='totalclicks'
textFontSize = '8pt'
bk.renderDonutChart(df95p, labels, values, textFontSize, hoverText, title);

rs.close()