import pandas as pd
import pandasql as pdsql

# add banners to the DF
def joinOnBannerId(currentDF, bannersMeta):
    sqlQuery = '''select
        A.*, 
        B.bannerSize,
        B.bannerType,
        B.deleted as bannerDeleted,
        B.BannerName,
        B.videoDuration
        from currentDF as A 
        inner join bannersMeta as B
        on A.BannerId = B.BannerId'''
    joinedDF = pdsql.sqldf(sqlQuery, locals())
    return joinedDF;


# add banners to the DF
def joinOnBrowserId(df, browsersMeta):
    sqlQuery = '''select
        A.*, 
        B.BrowserName
        from df as A 
        inner join browsersMeta as B
        on A.BrowserId = B.BrowserId'''

    joinedDF = pdsql.sqldf(sqlQuery, locals())
    return joinedDF;

# add campaigns to the DF
def joinOnCampaingId(df, campaignsMeta):
    sqlQuery = '''select
        A.*, 
        B.CampaignsName,
        B.visibilityArea
        from df as A 
        inner join campaignsMeta as B
        on A.CampaignId = B.CampaignsId'''

    joinedDF = pdsql.sqldf(sqlQuery, locals())
    return joinedDF;


# add  to the DF
def joinOnDeviceId(df, devicesMeta):
    # Join on Banners
    sqlQuery = '''select
        A.*, 
        B.DeviceName
        from df as A 
        inner join devicesMeta as B
        on A.DeviceTypeId = B.DeviceTypeId'''

    joinedDF = pdsql.sqldf(sqlQuery, locals())
    return joinedDF;

# add tagsMeta to the DF
def joinOnTagId(df, tagsMeta):
    sqlQuery = '''select
        A.*, 
        B.TagName
        from df as A 
        inner join tagsMeta as B
        on A.TagId = B.TagId'''

    joinedDF = pdsql.sqldf(sqlQuery, locals())
    return joinedDF;

# add clients to the DF
def joinOnClientId(df, clientsMeta):
    sqlQuery = '''select
        A.*, 
        B.ClientsName
        from df as A 
        inner join clientsMeta as B
        on A.ClientId = B.ClientsId'''

    joinedDF = pdsql.sqldf(sqlQuery, locals())
    return joinedDF;


# add placement to the DF
def joinOnPlacementId(df, placementsMeta):
    sqlQuery = '''select
        A.*, 
        B.PlacementsName
        from df as A 
        inner join placementsMeta as B
        on A.PlacementId = B.PlacementsId'''

    joinedDF = pdsql.sqldf(sqlQuery, locals())
    return joinedDF;

# add geolocation to the DF
def joinOnGeolocationId(df, geoMeta):
    sqlQuery = '''select
        A.*, 
        B.CityId,
        B.CityName,
        B.CountryName,
        B.RegionId,
        B.RegionCode,
        B.CountryId
        from df as A 
        inner join geoMeta as B
        on A.CityId = B.CityId'''
    joinedDF = pdsql.sqldf(sqlQuery, locals())
    return joinedDF;