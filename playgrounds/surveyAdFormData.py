import pandas as pd
import numpy as np
import os
import carlosUtils as cu
import bokehUtils as bk
import readMetaDefinitions as rd


# read json's
metaRoot = '/Users/carlos.aguilar/Documents/Beamly/Personalisation/adForm data/meta';

browsersMeta, devicesMeta, tagsMeta, bannersMeta, \
    campaignsMeta, clientsMeta = rd.readMetaFromJson(metaRoot);

# Set files to read
#clickName = 'Click_70172.csv';
#eventName = 'Event_70175.csv';
#imprName  = 'Impression_70175.csv';
#trackName = 'Trackingpoint_70171.csv';

#eventName = 'Event_70175.csv';
#imprName  = 'Impression_70179.csv';
#trackName = 'Trackingpoint_70171.csv';

clickName = 'Click_70260.csv';
eventName = 'Event_70487.csv';
imprName  = 'Impression_70407.csv';
trackName = 'Trackingpoint_70271.csv';


varsToBreakDown = ['BannerId-AdGroupId', \
'BrowserId', 'CampaignId', \
'CityId', 'ClientId', \
'CookieID', 'CookiesEnabled', \
'DeviceTypeId', \
'IsRobot', \
'PlacementId-ActivityId', 'TagId', \
'Timestamp', 'TransactionId']


# Read CLICK csv data
dataRoot  = '/Users/carlos.aguilar/Documents/Beamly/Personalisation/adForm data/raw';
fileName  = clickName
csvFile   = os.path.join(dataRoot, fileName);
df        = pd.read_csv(csvFile, delimiter='\t')
nR, nC    = df.shape
print('{} has got {} rows and {} columns'.format(fileName, nR, nC))
colNames = cu.printColumnNames(df);
# breakDown
#varName     = colNames[0];
#valElements = getElementCounts(df, varName,  minFreq=5);
valElements = cu.getElementCounts(df, 'IsRobot',  minFreq=5);

groupingVars = ['BrowserId', 'DeviceTypeId'];
grpDF        = cu.getAggregatedCounts(df, groupingVars[0], groupingVars[1])
grpDF        = grpDF.set_index(groupingVars[0]).join(browserMeta.set_index(groupingVars[0]));
grpDF        = grpDF.set_index(groupingVars[1]).join(devicesMeta.set_index(groupingVars[1]));

grpDF.sort_values(['total'], ascending=[0], inplace = True)

grpDF.head();
figTitle = fileName + ' grouped by ' +  groupingVars[0] + ',' + groupingVars[1]
bkFigure = bk.renderChordChart(grpDF, 'DeviceName', 'BrowserName', 'total', dfTitle = figTitle)
bk.exportFigure(bkFigure)

dfSource='DeviceName'
dfTarget='BrowserName'
dfValue= 'total'
chord_from_df = Chord(grpDF, 
source=dfSource, target=dfTarget, value=dfValue, 
title="simple line example",
x_axis_label='x')


# breakDown
cu.breakDownDF(df, varsToBreakDown, minFreq = 2);





# Read EVENT csv data
dataRoot  = '/Users/carlos.aguilar/Documents/Beamly/Personalisation/adForm data/raw';
fileName  = eventName
csvFile   = os.path.join(dataRoot, fileName);
df        = pd.read_csv(csvFile, delimiter='\t')
nR, nC    = df.shape
print('{} has got {} rows and {} columns'.format(fileName, nR, nC))
colNames = cu.printColumnNames(df);
# breakDown
#varName     = colNames[0];
#valElements = getElementCounts(df, varName,  minFreq=5);


groupingVars = ['BrowserId', 'DeviceTypeId'];
grpDF        = cu.getAggregatedCounts(df, groupingVars[0], groupingVars[1])
grpDF        = grpDF.set_index(groupingVars[0]).join(browserMeta.set_index(groupingVars[0]));
grpDF        = grpDF.set_index(groupingVars[1]).join(devicesMeta.set_index(groupingVars[1]));

grpDF.sort_values(['total'], ascending=[0], inplace = True)
grpDF.head();
figTitle = fileName + ' grouped by ' +  groupingVars[0] + ',' + groupingVars[1]
bkFigure = bk.renderChordChart(grpDF, 'DeviceName', 'BrowserName', 'total', dfTitle = figTitle)
bk.exportFigure(bkFigure, figTitle+'.png')


# breakDown
breakDownDF(df, varsToBreakDown, minFreq = 2);

xlsExt    = csvFile.replace("csv", "xlsx")
xlsFile   = os.path.join(dataRoot, xlsExt)
#dataFrameToXLS(df, xlsFile);





# Read IMPRESSION csv data

dataRoot  = '/Users/carlos.aguilar/Documents/Beamly/Personalisation/adForm data/raw';
fileName  = eventName
csvFile   = os.path.join(dataRoot, fileName);
df        = pd.read_csv(csvFile, delimiter='\t')
nR, nC    = df.shape
print('{} has got {} rows and {} columns'.format(fileName, nR, nC))
colNames = cu.printColumnNames(df);


groupingVars = ['BrowserId', 'DeviceTypeId'];
grpDF        = cu.getAggregatedCounts(df, groupingVars[0], groupingVars[1])
grpDF        = grpDF.set_index(groupingVars[0]).join(browserMeta.set_index(groupingVars[0]));
grpDF        = grpDF.set_index(groupingVars[1]).join(devicesMeta.set_index(groupingVars[1]));

grpDF.sort_values(['total'], ascending=[0], inplace = True)
grpDF.head();
figTitle = fileName + ' grouped by ' +  groupingVars[0] + ',' + groupingVars[1]
bk.renderChordChart(grpDF, 'DeviceName', 'BrowserName', 'total', dfTitle = figTitle)




xlsExt    = csvFile.replace("csv", "xlsx")
xlsFile   = os.path.join(dataRoot, xlsExt)
#dataFrameToXLS(df, xlsFile);


# Read TRACKPOINT csv data
dataRoot  = '/Users/carlos.aguilar/Documents/Beamly/Personalisation/adForm data/raw';
csvFile   = os.path.join(dataRoot, trackName);
df        = pd.read_csv(csvFile, delimiter='\t')
nR, nC    = df.shape
print('{} has got {} rows and {} columns'.format(trackName, nR, nC))
colNames = printColumnNames(df);
# breakDown
#varName     = colNames[0];
#valElements = getElementCounts(df, varName,  minFreq=5);

# breakDown
breakDownDF(df, varsToBreakDown, minFreq = 2);

xlsExt    = csvFile.replace("csv", "xlsx")
xlsFile   = os.path.join(dataRoot, xlsExt)
#dataFrameToXLS(df, xlsFile);






jsonFile     = 'clients.json'
jsonPath     = os.path.join(metaRoot, jsonFile);
clientMeta = pd.read_json(jsonPath);