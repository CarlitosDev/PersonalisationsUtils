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

resultsFolder = '/Users/carlos.aguilar/Documents/Beamly/Personalisation/clickResults';

# JUST ONE CAMPAIGN
saveFigure = True;


numCampaigns = 20;
sqlQuery = '''SELECT
A.campaign_id,
A.clientsname,
A.countryname,
A.devicename,
A.startdate,
A.enddate,
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
where B.rnk <= {}
group by 1,2,3,4,5,6,B.numTotals
order by totalclicks DESC'''.format(numCampaigns)


df = rs.query2DF(sqlQuery)


# Get a list of the most clicked campaigns
vars2show = ['campaign_id', 'clientsname', 'totalclicks', 'startdate', 'enddate']
df2 = df[vars2show].copy()
df2.drop_duplicates(inplace=True)

groupedDF = df.groupby(df['campaign_id'])


for name, group in groupedDF:
    groupClientName = group['clientsname'].iloc[0]
    print('Current campaign {} id {} got {} clicks'.format(groupClientName, \
        name, group['totalclicks'].sum()))
    # 
    totalCampaignClicks = group['totalclicks'].sum();
    group['perct'] = 100.0*group['totalclicks']/totalCampaignClicks;
    # cutoff at 98%
    idx95p = group['perct'].cumsum() < 98.0
    df95p  = group.loc[idx95p, :]

    fileName  = groupClientName + ' (id ' + str(name) + ').xlsx'
    xlsFile   = os.path.join(resultsFolder, fileName);
    cu.dataFrameToXLS(df95p, xlsFile);
    if saveFigure:
        # bokeh
        title  = groupClientName + \
            ' (id ' + str(name) + ') clicks: ' + str(totalCampaignClicks)
        labels = ['devicename','countryname']
        values = 'totalclicks'
        hoverText ='totalclicks'
        textFontSize = '8pt'
        bkFig = bk.renderDonutChart(df95p, labels, values, textFontSize, hoverText, title);
        fileName  = groupClientName + ' (id ' + str(name) + ').png'
        figFile   = os.path.join(resultsFolder, fileName);
        bk.exportFigure(bkFig, figFile);


rs.close()