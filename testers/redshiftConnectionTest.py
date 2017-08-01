
import redshift
import s3Utils


# Authenticate with Redshift using your db credentials.
rs = redshift.Redshift(user='carlos', password='4Beamly4')
sqlQuery = 'select * from pg_group;'
df = rs.query(sqlQuery)
rs.conn.close()





# skip redshift
import psycopg2
import pandas

user='carlos'
password='4Beamly4'

conn = psycopg2.connect(
    dbname='dev',
    port='5439',
    user=user,
    password=password,
    host='beamly-analytics.cbnxx53hhkkr.us-east-1.redshift.amazonaws.com'
    )
cursor = conn.cursor()


sqlQuery

cursor.execute(sqlQuery)
res = cursor.fetchall()
cols = [col[0] for col in cursor.description]
pd =  pandas.DataFrame(res, columns=cols)


conn.close();


# sqlAlchemy through psycopg2
import psycopg2
import pandas as pd
from sqlalchemy import create_engine


user='carlos'
password='4Beamly4'

conn = psycopg2.connect(
    dbname='dev',
    port='5439',
    user=user,
    password=password,
    host='beamly-analytics.cbnxx53hhkkr.us-east-1.redshift.amazonaws.com'
    )
dbEngine = create_engine('postgresql://', creator=conn)





dbname='dev'
port='5439'
user='carlos'
password='4Beamly4'
host='beamly-analytics.cbnxx53hhkkr.us-east-1.redshift.amazonaws.com'

preamble = 'postgresql://'
dbString = preamble + user + ':' + password + '@' + host + ':' + port + '/dev'
 
dbEngine = create_engine(dbString)
result_set = dbEngine.execute(sqlQuery)  



sqlQuery = 'select * from pg_group;'
test = pd.read_sql_query(sqlQuery, dbEngine)



from sqlalchemy.engine import url as urlConn

db_connect_url = urlConn.URL(
            drivername='postgresql',
            username=user,
            password=password,
            host='beamly-analytics.cbnxx53hhkkr.us-east-1.redshift.amazonaws.com',
            port='5439',
            database='dev',
)
dbEngine = create_engine(db_connect_url)

    dbname='dev',
   