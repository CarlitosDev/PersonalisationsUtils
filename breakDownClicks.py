import redshiftSqlAlchemy as rsa
import pandas as pd
import pandasql as pdsql
import os
import carlosUtils as cu

import bokehUtils as bk


# Authenticate with Redshift using your db credentials.
user     = 'carlos_aguilar'
password = 'MdogDI64j6vH90g973'
dbname   = 'adform'
host     = 'adform-ops.c7dxcjhlundm.eu-central-1.redshift.amazonaws.com'

# set up the folder to store the analysis
resultsFolder = '/Users/carlos.aguilar/Documents/Beamly/Personalisation/clickResults';

# set the table and schema
schemaName = 'adform'
tableName  = 'clickExtended'

# set the name of the variables to break-down
breakDownKeyWords = ['campaign_id', 'yyyy_mm_dd', 'click_detail_id_paid_keyword_id', \
'publisher_domain', 'destination_url', 'bannerid', 'bannerType', \
'client_id', 'device_type_id', 'placement_id_activity_id', 'tag_id', \
'city_id' ];

# flags to set the outputs
saveFigure    = True;
saveDFToExcel = True;


# connect and do some stuff
rs = rsa.RedshiftAlchemy(user=user, password=password, 
    database=dbname, host=host)

fileName  = schemaName + '.' + tableName + ' breakdown' + '.txt'
filePath  = os.path.join(resultsFolder, fileName);

fid = open(filePath, 'w')


# This snippet breaks down the variables and writes the counts to a file
for currentKey in breakDownKeyWords:
    sqlQuery = '''SELECT count(distinct({})) as numRecords 
        from {}.{}'''.format(currentKey, schemaName, tableName)

    df = rs.query2DF(sqlQuery);
    cu.printf('variable {} has got {} unique values...'.format(currentKey, df.numrecords[0]), fid=fid)



# This bit takes the 20 most clicked campaigns and writes the results
numCampaigns = 20;
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
where B.rnk <= {}
group by 1,2,3,4, B.numTotals
order by 1,2,totalclicks DESC'''.format(numCampaigns)

df        = rs.query2DF(sqlQuery)
groupedDF = df.groupby(df['campaign_id'])
idx       = 0;


for name, group in groupedDF:
    idx += 1;
    groupClientName = group['clientsname'].iloc[0]
    cu.printf('{} - Processing {}...'.format(idx, groupClientName))
    print('Current campaign {} id {} got {} clicks'.format(groupClientName, \
        name, group['totalclicks'].sum()))
    # 
    totalCampaignClicks = group['totalclicks'].sum();
    group['perct'] = 100.0*group['totalclicks']/totalCampaignClicks;
    # cutoff at 95%
    idx95p = group['perct'].cumsum() < 96.0
    df95p  = group.loc[idx95p, :]

    fileName  = groupClientName + ' (id ' + str(name) + ').xlsx'
    xlsFile   = os.path.join(resultsFolder, fileName);

    # Append to the current break-down file
    #cu.printf(df95p.to_string(index = False),  fid=fid)
    if idx==1:
        df95p.to_csv(fid, mode='a', header=True, index = False)
    else:
        df95p.to_csv(fid, mode='a', header=False, index = False)

    if saveDFToExcel:
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

fid.close()



import os
import sys
pythonModsRoot =  '/Users/carlos.aguilar/Google Drive/PythonDev/Coding/BeamlyPython'
pythonModules  =  ['redshiftSqlAlchemy', 'carlosUtils']

for currentModule in pythonModules:
    xlsFile   = os.path.join(pythonModsRoot, currentModule, '.py'); 

module_path = os.path.abspath(os.path.join('..'))
if pythonModsRoot not in sys.path:
    sys.path.append(pythonModsRoot)