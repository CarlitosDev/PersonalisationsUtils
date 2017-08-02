INSERT INTO adform.impressionsClicksMapped

SELECT DISTINCT

--
tbClicks.client_id,
tableClients.clientsname,

tbClicks.campaign_id,
tableB.bioniccampaignid,

tableB.campaignstatus,

tableB.market,
tableB.masterbrand,
tableB.brandbis,
tableB."brand+campaignevent",

tableB.agency,
tableB.clientname,
tableB.division,
tableB.campaignobjective,
tableB.campaignsname,

--
tableB.year,
tableB.quarter,
tableB.startdate,
tableB.enddate,
tableB.type,
--
tbClicks.yyyy_mm_dd,
tbClicks."timestamp"         as clicksTimestamp,
tbImpressions."timestamp"    as impressionsTimestamp,
tbImpressions.transaction_id as transaction_id_IMP,
--
tbClicks.tag_id,
tbClicks.banner_id_ad_group_id,
tbClicks.click_detail_id_paid_keyword_id,
-- let's keep the transaction
tbClicks.transaction_id,
-- Source and destination. Some regEx to be used here
tbClicks.publisher_domain,
tbImpressions.publisher_domain as imp_publisher_domain,
tbClicks.publisher_url,
tbClicks.destination_url,
--
tbClicks.placement_id_activity_id,
tbClicks.cookies_enabled,
tbClicks.is_robot,
--
tbClicks.browser_id,
tbBrowsers.BrowserName,

--
tbClicks.device_type_id,
tableDevices.devicename,
--
tbClicks.ip,
--
tbClicks.city_id,
tableCity.cityname,
tableCity.regionid,
tableCity.regioncode,
tableCity.countryid,
tableCity.countryname,
--
tbClicks.visibility1_flag,
tbClicks.visibility_time,

case
  when tbImpressions.transaction_id is NULL then 0
  else 1
END as matchingImpression

--,tbImpressions.*

from adform.clicks as tbClicks

left join adform.impressions as tbImpressions
  ON  tbClicks.banner_id_ad_group_id    = tbImpressions.banner_id_ad_group_id
  and tbClicks.browser_id               = tbImpressions.browser_id
  and tbClicks.campaign_id              = tbImpressions.campaign_id
  and tbClicks.city_id                  = tbImpressions.city_id
  and tbClicks.client_id                = tbImpressions.client_id
  and tbClicks.ip                       = tbImpressions.ip
  and tbClicks.placement_id_activity_id = tbImpressions.placement_id_activity_id
  and tbClicks.rotator_id               = tbImpressions.rotator_id
  and tbClicks.tag_id                   = tbImpressions.tag_id
  and tbClicks.yyyy_mm_dd               = tbImpressions.yyyy_mm_dd
  -- sensible restriction
  and tbImpressions."timestamp"        <= tbClicks."timestamp"
  -- add a 8 mins restriction
  and tbImpressions."timestamp"         > dateadd(min, -8, tbClicks."timestamp")

-- Use meta information to complete
inner join adform.meta_campaigns_v2 as tableB
  on cast(tbClicks.campaign_id as INTEGER) = tableB.campaign_id

inner join adform.meta_browsers as tbBrowsers
  on cast(tbClicks.browser_id as INTEGER) = tbBrowsers.browserid

inner join adform.meta_clients as tableClients
  on cast(tbClicks.client_id as INTEGER) = tableClients.clientsid

inner join adform.meta_devices as tableDevices
  on cast(tbClicks.device_type_id as INTEGER) = tableDevices.devicetypeid

left join adform.meta_mini_geo as tableCity
  on cast(tbClicks.city_id as INTEGER) = tableCity.city_id


where tbClicks.campaign_id in (919044)


/*
delete adform.impressionsClicksMapped
where campaign_id in (886811)
 */