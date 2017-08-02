import os
import sys
# Do this trick to add the new Beamly functionality
pythonModsRoot = '/Users/carlos.aguilar/Documents/BeamlyRepos/PersonalisationsUtils'
if pythonModsRoot not in sys.path:
    sys.path.append(pythonModsRoot)

import redshiftSqlAlchemy as rsa
import pandas as pd
import pandasql as pdsql
import joinMetaDefinitions as jn
import carlosUtils as cu

sqlRoot   = '/Users/carlos.aguilar/Documents/BeamlyRepos/PersonalisationsUtils/sql';
fileName  = 'templateInsertCampaignData.sql'
filePath  = os.path.join(sqlRoot, fileName)

sqlQuery = cu.readTextFile(filePath);




# Authenticate with Redshift using your db credentials.
user     = 'carlos_aguilar'
password = 'MdogDI64j6vH90g973'
dbname   = 'adform'
host     = 'adform-ops.c7dxcjhlundm.eu-central-1.redshift.amazonaws.com'

rs = rsa.RedshiftAlchemy(user=user, password=password, 
    database=dbname, host=host)


conn = rs.dbEngine.connect()

listOfCampaigns = ['886811', '929026', '918140', \
'950281', '906824', '922742','912694', '894699', '894699', '875443']

for currentCampaign in listOfCampaigns:
    print('Running {}...'.format(currentCampaign))
    currentQuery = str.replace(sqlQuery, "919044", currentCampaign)
    conn.execute(currentQuery)

rs.close();