import pandas as pd
import os

# read json's
def readMetaFromJson(metaRoot):

    jsonFile     = 'browsers.json'
    jsonPath     = os.path.join(metaRoot, jsonFile);
    browsersMeta = pd.read_json(jsonPath);
    browsersMeta.rename(columns={'id': 'BrowserId', 'name': 'BrowserName'}, inplace=True)


    jsonFile    = 'devices.json'
    jsonPath    = os.path.join(metaRoot, jsonFile);
    devicesMeta = pd.read_json(jsonPath);
    devicesMeta.rename(columns={'id': 'DeviceTypeId', 'name': 'DeviceName'}, inplace=True)


    jsonFile    = 'tags.json'
    jsonPath    = os.path.join(metaRoot, jsonFile);
    tagsMeta    = pd.read_json(jsonPath);
    tagsMeta.rename(columns={'id': 'TagId', 'name': 'TagName'}, inplace=True)


    jsonFile    = 'banners-adgroups.json'
    jsonPath    = os.path.join(metaRoot, jsonFile);
    bannersMeta = pd.read_json(jsonPath);
    bannersMeta.rename(columns={'id': 'BannerId', 'name': 'BannerName'}, inplace=True);


    jsonFile      = 'campaigns.json'
    jsonPath      = os.path.join(metaRoot, jsonFile);
    campaignsMeta = pd.read_json(jsonPath);
    campaignsMeta.rename(columns={'id': 'CampaignsId', 'name': 'CampaignsName'}, inplace=True);


    jsonFile     = 'clients.json'
    jsonPath     = os.path.join(metaRoot, jsonFile);
    clientsMeta  = pd.read_json(jsonPath);
    clientsMeta.rename(columns={'id': 'ClientsId', 'name': 'ClientsName'}, inplace=True);


    jsonFile     = 'placements-activities.json'
    jsonPath     = os.path.join(metaRoot, jsonFile);
    placementsMeta  = pd.read_json(jsonPath);
    placementsMeta.rename(columns={'id': 'PlacementsId', 'name': 'PlacementsName'}, inplace=True);



    jsonFile     = 'geolocations.json'
    jsonPath     = os.path.join(metaRoot, jsonFile);
    geoMeta      = pd.read_json(jsonPath);
    geoLocations = {'cityId': 'CityId', 'city': 'CityName', 'country': 'CountryName', \
    'regionId': 'RegionId', 'regionCode': 'RegionCode', 'countryId': 'CountryId'}
    geoMeta.rename(columns=geoLocations, inplace=True);

    return browsersMeta, devicesMeta, tagsMeta, bannersMeta, campaignsMeta, \
clientsMeta, placementsMeta, geoMeta