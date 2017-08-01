# skip redshift
import psycopg2
import pandas

user='carlos_aguilar'
password='MdogDI64j6vH90g973'

conn = psycopg2.connect(
    dbname='adform',
    port='5439',
    user=user,
    password=password,
    host='adform-ops.c7dxcjhlundm.eu-central-1.redshift.amazonaws.com'
    )
cursor = conn.cursor()


   host='beamly-analytics.cbnxx53hhkkr.us-east-1.redshift.amazonaws.com'