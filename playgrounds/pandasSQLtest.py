import pandas as pd
import numpy as np
import os
import carlosUtils as cu
import bokehUtils as bk
import readMetaDefinitions as rd
from pandasql import sqldf


pysqldf = lambda q: sqldf(q, globals())

# read json's
metaRoot = '/Users/carlos.aguilar/Documents/Beamly/Personalisation/adForm data/meta';

browsersMeta, devicesMeta, tagsMeta, bannersMeta, \
    campaignsMeta, clientsMeta = rd.readMetaFromJson(metaRoot);


# test out pandassql funcionality
sqlQuery = 'select * from browsersMeta;'

print(pysqldf(sqlQuery).head())


sqlQuery = '''select distinct
    A.BrowserId, B.BrowserName, count(*) as numCounts
    from df as A 
    inner join browsersMeta as B
    on A.BrowserId = B.BrowserId'''


joined = pysqldf(sqlQuery)
joined.head()



# Join on Banners
sqlQuery = '''select
    A.*, 
    B.bannerSize,
    B.bannerType,
    B.deleted as bannerDeleted,
    B.BannerName,
    B.videoDuration
    from df as A 
    inner join bannersMeta as B
    on A.BannerId-AdGroupId = B.BannerId'''


joined = pysqldf(sqlQuery)
joined.head()


# Join on Banners
# Join on Banners
sqlQuery = '''select
    A.*, 
    B.BrowserName
    from df as A 
    inner join browsersMeta as B
    on A.BrowserId = B.BrowserId'''

joinedDF = pysqldf(sqlQuery)



sqlQuery = '''select
    A.*, 
    B.CampaignsName,
    B.visibilityArea
    from df as A 
    inner join campaignsMeta as B
    on A.CampaignId = B.CampaignsId'''

joinedDF = pysqldf(sqlQuery)


sqlQuery = '''select
    A.*, 
    B.TagName
    from df as A 
    inner join tagsMeta as B
    on A.TagId = B.TagId'''

joinedDF = pysqldf(sqlQuery)