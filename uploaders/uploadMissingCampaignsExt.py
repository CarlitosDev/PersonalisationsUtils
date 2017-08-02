"""

    Upload the missing definitions

"""

import os

import redshiftSqlAlchemy as rsa
import pandas as pd
import pandasql as pdsql
import joinMetaDefinitions as jn
import carlosUtils as cu
import re
import bokehUtils as bk


metaRoot     = '/Users/carlos.aguilar/Documents/Beamly/Personalisation/'
xlsFile      = 'summaryData.xlsx'
xlsFilePath  = os.path.join(metaRoot, xlsFile);
df = pd.read_excel(xlsFilePath, sheetname='Sheet4');

# Authenticate with Redshift using your db credentials.
user     = 'carlos_aguilar'
password = 'MdogDI64j6vH90g973'
dbname   = 'adform'
host     = 'adform-ops.c7dxcjhlundm.eu-central-1.redshift.amazonaws.com'

rs = rsa.RedshiftAlchemy(user=user, password=password, 
    database=dbname, host=host)

tableName = 'meta_ext_temp'
df.to_sql(tableName, rs.getDBEngine(), 
    schema='adform', 
    index = False, 
    if_exists = 'replace', 
    chunksize = 100)
print('Table {} uploaded...'.format(tableName))


rs.close()