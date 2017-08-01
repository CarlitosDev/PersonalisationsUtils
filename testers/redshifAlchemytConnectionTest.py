import redshiftSqlAlchemy as rsa
import pandas as pd


# Authenticate with Redshift using your db credentials.
rs = rsa.RedshiftAlchemy(user='carlos', password='4Beamly4')

sqlQuery = 'select cast(city_id as INTEGER) as city_id, count(*) as numTimes from adform.cityList group by 1'
df       = rs.query2DF(sqlQuery)
df.head()






rs.close()


