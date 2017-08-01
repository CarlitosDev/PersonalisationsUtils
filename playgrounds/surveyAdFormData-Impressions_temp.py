import pandas as pd
import numpy as np
import os
import carlosUtils as cu
import bokehUtils as bk
import readMetaDefinitions as rd
import joinMetaDefinitions as jn
import redshiftSqlAlchemy as rsa


# read json's
metaRoot = '/Users/carlos.aguilar/Documents/Beamly/Personalisation/adForm data/meta';
dataRoot = '/Users/carlos.aguilar/Documents/Beamly/Personalisation/adForm data/raw';

browsersMeta, devicesMeta, tagsMeta, bannersMeta, \
    campaignsMeta, clientsMeta, placementsMeta, geoMeta = rd.readMetaFromJson(metaRoot);

# Set files to read
imprName  = 'Impression_70175.csv';

varsToBreakDown = ['bannerType', 'BannerName', \
'BrowserName', 'CampaignsName', \
'CountryName',  'ClientsName', \
'CookiesEnabled', \
'DeviceName', \
'IsRobot', \
'PlacementsName', 'TagName']

# Read IMPRESSION csv data - rename the field 'banner' field as it crashes the sqlQuery
fileName      = imprName
csvFile       = os.path.join(dataRoot, fileName);
df            = pd.read_csv(csvFile, delimiter='\t')
colsToReplace = {'BannerId-AdGroupId': 'BannerId', 'PlacementId-ActivityId': 'PlacementId'}
df.rename(columns=colsToReplace, inplace=True)
 

nR, nC = df.shape
print('{} has got {} rows and {} columns'.format(fileName, nR, nC))
#colNames = cu.printColumnNames(df);

# Join on the metadata to get the definitions (use pandasql)
currentDF = df.copy();

currentDF = jn.joinOnBrowserId(currentDF    , browsersMeta)
currentDF = jn.joinOnCampaingId(currentDF   , campaignsMeta)
currentDF = jn.joinOnDeviceId(currentDF     , devicesMeta)
currentDF = jn.joinOnTagId(currentDF        , tagsMeta)
currentDF = jn.joinOnClientId(currentDF     , clientsMeta)
currentDF = jn.joinOnBannerId(currentDF     , bannersMeta)
currentDF = jn.joinOnPlacementId(currentDF  , placementsMeta)
currentDF = jn.joinOnGeolocationId(currentDF, geoMeta)


xlsFile   = csvFile.replace('csv', 'xlsx')
#cu.dataFrameToXLS(currentDF, xlsFile);

nR, nC    = currentDF.shape
print('{} has got {} rows and {} columns'.format('currentDF', nR, nC))


# breakDown
cu.breakDownDF(currentDF, varsToBreakDown, minPerc = 5);