import pandas as pd
from sqlalchemy.engine import url as urlConn
from sqlalchemy import create_engine

class RedshiftAlchemy():
    def __init__(self, user, password, 
        host='beamly-analytics.cbnxx53hhkkr.us-east-1.redshift.amazonaws.com', 
        database = 'dev'):
        urlDBConn = urlConn.URL(
                drivername='postgresql',
                username=user,
                password=password,
                host = host,
                port='5439',
                database=database
                )
        dbEngine = create_engine(urlDBConn)
        self.dbEngine = dbEngine

    def query2DF(self, sqlQuery):
        return(pd.read_sql_query(sqlQuery, self.dbEngine))

    def close(self):
       self.dbEngine.connect().close()

    def getDBEngine(self):
       return self.dbEngine

    def experimentalInsert(self, df, tableName, schemaName):
        from sqlalchemy import Table, MetaData
        engine        = self.dbEngine
        conn          = engine.connect()
        metadata      = MetaData(engine, reflect=True, schema=schemaName)
        listToWrite   = df.to_dict(orient='records')
        tblReflection = Table(tableName, metadata, autoload=True, autoload_with=engine)
        conn.execute(tblReflection.insert(), listToWrite)