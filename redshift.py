import psycopg2
import pandas


class Redshift():
    def __init__(self, user, password):
        self.conn = psycopg2.connect(
            dbname='dev',
            port='5439',
            user=user,
            password=password,
            host='beamly-analytics.cbnxx53hhkkr.us-east-1.redshift.amazonaws.com'
        )
        self.cursor = self.conn.cursor()

    def query(self, sql):
        self.cursor.execute(sql)
        res = self.cursor.fetchall()
        cols = [col[0] for col in self.cursor.description]
        return pandas.DataFrame(res, columns=cols)
