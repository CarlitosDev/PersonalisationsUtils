import pandas as pd
import numpy as np
import os
from getElementCounts import getElementCounts
from sklearn.model_selection import train_test_split


fName     = 'Survey Data Clean CA.xlsx';
xlsRoot   = '/Users/carlos.aguilar/Documents/Beamly/Fragance Finder/charlotte-FF';
xlsFile   = os.path.join(xlsRoot, fName)
df        = pd.read_excel(xlsFile, sheetname = 'value clean')
colNames  = df.keys().tolist();


for iVar in colNames:
    print('{}'.format(iVar))


varsOfInterest = ['Age', 'Gender', 'Current Location Region', 'Current Location Type', \
'General Fragrances Quantity','General Fragrances Frequency','Marital Status', 'Employed', \
'Employment Status'];

y = df['Current Fragrance House'];
X = df[varsOfInterest].copy();

iVar = varsOfInterest[0];
_    = getElementCounts(df, 'Current Fragrance House');

print('Class labels:', np.unique(y))



iVar = varsOfInterest[0];
_    = getElementCounts(df1, iVar);

# get the female/male ratio
getElementCounts(df, 'Employed');

# get the frequency of using fragrance
getElementCounts(df, 'a5');

# get the brands
getElementCounts(df, 'pref1x2_2');




for iVar in colNames():
    # this thing returns a 'series' object
    a = df[iVar].value_counts();
    for k in np.arange(0, len(a))
        printf('Value {} appear {}\n'.format( a.index[k],  a.iloc[k])
        
       
iVar = colNames[4]
# this thing returns a 'series' object
a = df[iVar].value_counts();
for k in np.arange(0, len(a)):
    print('Value {} appears {} times'.format( a.index[k],  a.iloc[k]))
   


dfStats = df.describe();

a = df.apply(pd.Series.nunique)
b = df.unstack().groupby(level=0).nunique