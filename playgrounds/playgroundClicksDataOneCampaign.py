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



sqlQuery = '''SELECT
A.campaign_id,
A.clientsname,
A.countryname,
A.devicename,
sum(A.numRecords) as totalClicks,
B.numTotals
from adform.clickExtended as A
inner join (
  SELECT  campaign_id,
  sum(numRecords) AS numTotals,
  rank() over (order by numTotals desc) as rnk
  from adform.clickExtended
  group by 1
) as B
on A.campaign_id = B.campaign_id
where B.rnk <= 10
group by 1,2,3,4, B.numTotals
order by 1,2,totalclicks DESC'''


df = rs.query2DF(sqlQuery)


groupedDF = df.groupby(df['campaign_id'])




for name, group in groupedDF:
    groupClientName = group['clientsname'].iloc[0]
    print('Current campaign {} id {} got {} clicks'.format(groupClientName, \
        name, group['totalclicks'].sum()))
    # 
    totalCampaignClicks = group['totalclicks'].sum();
    group['perct'] = 100.0*group['totalclicks']/totalCampaignClicks;
    # cutoff at 95%
    idx95p = group['perct'].cumsum() < 96.0
    df95p  = group.loc[idx95p, :]
    # bokeh
    title  = groupClientName + \
        ' (id ' + str(name) + ') clicks: ' + str(totalCampaignClicks)
    labels = ['devicename','countryname']
    values='totalclicks'
    hoverText ='totalclicks'
    textFontSize = '8pt'
    bk.renderDonutChart(df95p, labels, values, textFontSize, hoverText, title);

    












# Also, you can extract into a dictionary and index v b y the key value
v = dict(list(groupedDF))




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