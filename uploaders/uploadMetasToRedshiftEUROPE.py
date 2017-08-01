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
    campaignsMeta, clientsMeta, placementsMeta, geoMeta = rd.readMetaFromJson(metaRoot)


# Authenticate with Redshift using your db credentials.
user     = 'carlos_aguilar'
password = 'MdogDI64j6vH90g973'
dbname   = 'adform'
host     = 'adform-ops.c7dxcjhlundm.eu-central-1.redshift.amazonaws.com'

rs = rsa.RedshiftAlchemy(user=user, password=password, 
    database=dbname, host=host)


tableName = 'meta_browsers'
browsersMeta.to_sql(tableName, rs.getDBEngine(), 
    schema='adform', 
    index = False, 
    if_exists = 'replace', 
    chunksize = 100)
print('Table {} uploaded...'.format(tableName))

tableName = 'meta_devices'
devicesMeta.to_sql(tableName, rs.getDBEngine(), 
    schema='adform', 
    index = False, 
    if_exists = 'replace', 
    chunksize = 100)
print('Table {} uploaded...'.format(tableName))

# blocked in here
tableName = 'meta_tags'
tagsMeta.to_sql(tableName, rs.getDBEngine(), 
    schema='adform', 
    index = False, 
    if_exists = 'replace', 
    chunksize = 100)
print('Table {} uploaded...'.format(tableName))

tableName = 'meta_banners'
bannersMeta.to_sql(tableName, rs.getDBEngine(), 
    schema='adform', 
    index = False, 
    if_exists = 'replace', 
    chunksize = 100)
print('Table {} uploaded...'.format(tableName))

tableName = 'meta_campaigns'
campaignsMeta.to_sql(tableName, rs.getDBEngine(), 
    schema='adform', 
    index = False, 
    if_exists = 'replace', 
    chunksize = 100)
print('Table {} uploaded...'.format(tableName))

tableName = 'meta_clients'
clientsMeta.to_sql(tableName, rs.getDBEngine(), 
    schema='adform', 
    index = False, 
    if_exists = 'replace', 
    chunksize = 100)
print('Table {} uploaded...'.format(tableName))

tableName = 'meta_placements'
placementsMeta.to_sql(tableName, rs.getDBEngine(), 
    schema='adform', 
    index = False, 
    if_exists = 'replace', 
    chunksize = 100)
print('Table {} uploaded...'.format(tableName))

'''
tableName = 'meta_geo'
geoMeta.to_sql(tableName, rs.getDBEngine(), 
    schema='adform', 
    index = False, 
    if_exists = 'replace', 
    chunksize = 100)
print('Table {} uploaded...'.format(tableName))
'''

rs.close()