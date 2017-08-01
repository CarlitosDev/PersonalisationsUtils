import redshiftSqlAlchemy as rsa
import pandas as pd
import pandasql as pdsql
import joinMetaDefinitions as jn
import os
import carlosUtils as cu
import math


print('Reading {} from Redshift...'.format('adform.clickExtended'))

# The definitions in geo.json extend to 0.5 M records. Let's match on the cities that we have already got in the dataset
# Authenticate with Redshift using your db credentials.
user     = 'carlos_aguilar'
password = 'MdogDI64j6vH90g973'
dbname   = 'adform'
host     = 'adform-ops.c7dxcjhlundm.eu-central-1.redshift.amazonaws.com'

rs = rsa.RedshiftAlchemy(user=user, password=password, 
    database=dbname, host=host)



sqlQuery = 'select DISTINCT city_id from adform.clickExtended WHERE cityname is null'
df       = rs.query2DF(sqlQuery)


# Let's read the geo file and join on the present cities
metaRoot     = '/Users/carlos.aguilar/Documents/Beamly/Personalisation/adForm data/meta';
jsonFile     = 'geolocations.json'
print('Reading {} ...'.format(jsonFile))
jsonPath     = os.path.join(metaRoot, jsonFile);
geoMeta      = pd.read_json(jsonPath);
geoLocations = {'cityId': 'CityId', 'city': 'CityName', 'country': 'CountryName', \
'regionId': 'RegionId', 'regionCode': 'RegionCode', 'countryId': 'CountryId'}
geoMeta.rename(columns=geoLocations, inplace=True);


sqlQuery = '''SELECT DISTINCT
    A.city_id,
    B.CityName,
    B.CountryName,
    B.RegionId,
    B.RegionCode,
    B.CountryId
    from df as A 
    inner join geoMeta as B
    on A.city_id = B.CityId'''
joinedDF = pdsql.sqldf(sqlQuery, locals())

print('Shrinking to {} cities...'.format(len(joinedDF)))

tableName = 'meta_mini_geo'
numRows   = len(joinedDF);
numChunks = 50
chunkSize = math.floor(numRows/numChunks)

for i in range(0, numChunks):
    a = i * chunkSize
    b = a + chunkSize
    print('Processing {}/{}. From {} to {}...'.format(i, numChunks,a,b))
    joinedDF.loc[a:b, :].to_sql(tableName, rs.getDBEngine(), 
        schema='adform', 
        index = False, 
        if_exists = 'append')

rs.close()