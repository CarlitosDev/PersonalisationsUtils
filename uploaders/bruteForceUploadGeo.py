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

# Authenticate with Redshift using your db credentials.
rs = rsa.RedshiftAlchemy(user='carlos', password='4Beamly4')

jsonFile     = 'geolocations.json'
jsonPath     = os.path.join(metaRoot, jsonFile);
geoMeta      = pd.read_json(jsonPath);
geoMeta.head()


# ------------


tableName = 'meta_geo'
df        = geoMeta.copy()
numRows   = len(df);
numChunks = 100
chunkSize = numRows/numChunks

for i in range(0, numChunks):
    a = i
    b = a + chunkSize
    print('Processing {}/{}...'.format(i, numChunks))
    if i==0:
        df.loc[a:b, :].to_sql(tableName, rs.getDBEngine(), 
            schema='adform', 
            index = False, 
            if_exists = 'replace')
    else:
        df.loc[a:b, :].to_sql(tableName, rs.getDBEngine(), 
            schema='adform', 
            index = False, 
            if_exists = 'append')


rs.close()