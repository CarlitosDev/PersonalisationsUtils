import pandas as pd
import numpy as np
import os
import pickle
import sys

def getElementCounts(df, varName,  minPerc = 5):

    a = df[varName].value_counts();
    total = a.sum();
    percScale = 100.0/total
    for k in np.arange(0, len(a)):
        if a.iloc[k]*percScale >= minPerc:
            print('Value {} appears {} times and covers {:.2f} percent'.format(a.index[k], a.iloc[k], a.iloc[k]*percScale))
    return a;

def getAggregatedCounts(df, varA, varB):
    b  = df.groupby([varA, varB], as_index=False)[varA].agg({'total': 'count'}).copy();
    return b;

def breakDownDF(df, listOfVars, minPerc = 2):
    for iVar in listOfVars:
        print('\nBreaking down {}'.format(iVar))
        _ = getElementCounts(df, iVar,  minPerc)

def dataFrameToPickle(df, currentFile):
    if not df.dropna().empty:
        with open(currentFile,'wb') as f:
            pickle.dump(df,f)

def dataFrameToXLS(df, xlsFile, sheetName = 'DF'):
    if not df.empty:
        xlsWriter = pd.ExcelWriter(xlsFile);
        df.to_excel(xlsWriter, sheetName, index = False);
        xlsWriter.save();

def dataFrameToCreateTableSQL(df):
    if not df.empty:
        sqlCreateTable = pd.io.sql.get_schema(df.reset_index(), 'data')
        print(sqlCreateTable)
        return sqlCreateTable;
    
def printColumnNames(df):
    colNames    = df.keys().tolist();
    strColNames = sorted(colNames,  key=str.lower)
    for iCol in strColNames:
        print('{}'.format(iCol))

    return colNames;

def getObjClass(obj):
    className = obj.__class__.__name__
    print(className);
    return className;

def clearInt():
    os.system('clear')

def str2File(thisStr, thisFile):
    f = open(thisFile, 'w')
    f.write(thisStr)
    f.close()
        
def printf(thisStr, fid = sys.stderr):
    print(thisStr, file=fid)

def getDFfromRegEx(df, varName, regExPattern, colNames):
    '''
    Apply a regEx to a DF and get a DF as result
    '''
    import re
    regEx  = re.compile(regExPattern)
    tempDF = df[varName].str.extract(regEx, expand=True);
    tempDF.columns = colNames;

    return tempDF;

def readTextFile(filePath):
    with open(filePath) as in_file:
        textData = in_file.read()
    return textData