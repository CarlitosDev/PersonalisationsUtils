/*

    The merging of the campaign tables:
      - meta_campaigns (from uploadMetasToRedShift.py)
      - meta_ext_campaign (extendCampaignInformation.sql + playgroundGetCampaignFields.py)

*/


SELECT

top 100 * 
--from adform.impressionsClicksMapped as tbClkImp
from adform.clicks as tbClkImp

left join adform.tracking_points as tbTracking
  ON  tbClkImp.banner_id_ad_group_id    = tbTracking.banner_id_ad_group_id
  and tbClkImp.browser_id               = tbTracking.browser_id
  and tbClkImp.campaign_id              = tbTracking.campaign_id
  and tbClkImp.city_id                  = tbTracking.city_id
  and tbClkImp.client_id                = tbTracking.client_id
  and tbClkImp.ip                       = tbTracking.ip
  and tbClkImp.placement_id_activity_id = tbTracking.placement_id__activity_id
  and tbClkImp.rotator_id               = tbTracking.rotator_id
  and tbClkImp.tag_id                   = tbTracking.tag_id
  and tbClkImp.yyyy_mm_dd               = tbTracking.yyyy_mm_dd

where tbTracking.tag_id is not null

-------------
drop TABLE adform.impressionsClicksMapped;
drop TABLE adform.impressionstemp2;

create TABLE adform.impressionsClicksMapped DISTKEY(campaign_id)  COMPOUND SORTKEY (yyyy_mm_dd, campaign_id)
AS
(
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
where tbClicks.campaign_id in (919044, 897248, 886811, 929026, 918140,
950281, 906824, 922742,912694, 894699, 894699, 875443)
*/
)


SELECT TOP 1000 *
FROM adform.impressionsClicksMapped
WHERE campaign_id = 929026
order by yyyy_mm_dd ASC


SELECT top 15 distinct campaignsname
FROM adform.impressionsClicksMapped



--
select
campaign_id,
campaignsname,
sum(matchingimpression) as totalMatched,
count(*) as totals
from adform.impressionsClicksMapped
GROUP BY 1,2



select top 10 * from adform.meta_mini_geo


select top 10 * from adform.tracking_points
where campaign_id in (897248)
---

---
drop TABLE adform.impressions3Clicks;
-----

select
campaign_id,
campaignsname,
sum( case when TRIM(publisher_domain)='' then 0 else 1 end ) as validPublisher,
sum(matchingimpression) as impressionlMatched,
count(*) as totals
from adform.impressionsClicksMapped
GROUP BY 1,2



SELECT TOP 100 *
from adform.impressionsClicksMapped


-- Find some sort of relationship between impressions and clicks for a campaign, for one day
SELECT * FROM
adform.clicks
where campaign_id = 875443
and "timestamp" BETWEEN '2017-06-22  00:00:07' and '2017-06-22  00:02:07'


SELECT * FROM
adform.impressions
where campaign_id = 875443
and "timestamp" BETWEEN '2017-06-22  00:00:00' and dateadd(min,3, (select timestamp '2017-06-22  00:00:00'))
and city_id in ('11674',	'16546',	'16546',	'771',	'2025',	'858',	'41',	'924',	'4656',	'16546',	'199',	'231')

order by "timestamp" ASC




SELECT
tbClicks.*,
tbImpressions.transaction_id as transaction_id_IMP
from adform.clicks as tbClicks
left join adform.impressionstemp1 as tbImpressions
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
-- As many impressions are served, let's set a 5 min window from impression to click
and tbImpressions."timestamp"  BETWEEN tbClicks."timestamp" and dateadd(min, -5, tbClicks."timestamp")

where tbClicks.campaign_id = 875443
and tbClicks.yyyy_mm_dd = '2017-06-22'



SELECT *
from adform.clicks as tbClicks
where tbClicks.campaign_id = 875443
and tbClicks.yyyy_mm_dd = '2017-06-22'

SELECT count(*) as numElements
from adform.clicks as tbClicks
where tbClicks.campaign_id = 875443
and tbClicks.yyyy_mm_dd = '2017-06-22'

SELECT *
from adform.impressions
where transaction_id in
('2062215657901','2102781187607',
'2102781195807','2102781202407',
'2102781198707')
and  campaign_id = 875443
and yyyy_mm_dd = '2017-06-22'


-------------
drop TABLE adform.impressions2Clicks;

create TABLE adform.impressions2Clicks
AS
(
SELECT DISTINCT
tbClicks.yyyy_mm_dd,
tbClicks.transaction_id,
tbClicks.cookie_id,
tbClicks.tag_id,
tbClicks.rotator_id,
tbClicks.banner_id_ad_group_id,
tbClicks.click_detail_id_paid_keyword_id,
tbClicks.publisher_domain,
tbClicks.campaign_id,
tbClicks.placement_id_activity_id,
tbClicks.cookies_enabled,
tbClicks.is_robot,
tbClicks.publisher_url,
tbClicks.destination_url,
tbClicks.ip,
tbClicks.device_type_id,
tbClicks.client_id,
tbClicks.city_id,
tbClicks.browser_id,
tbClicks.visibility1_flag,
tbClicks.visibility_time,

tbClicks."timestamp" as clicksTimestamp,
tbImpressions."timestamp" as impressionsTimestamp,
tbImpressions.transaction_id as transaction_id_IMP,
case when tbImpressions.transaction_id is NULL then 0
else 1
END as matchingImpression

--,tbImpressions.*

from adform.clicks as tbClicks
left join adform.impressionstemp1 as tbImpressions
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

  and tbImpressions."timestamp"  <= tbClicks."timestamp"

where tbClicks.campaign_id = 875443

  )
-- As many impressions are served, let's set a 5 min window from impression to click
--and tbImpressions."timestamp"  BETWEEN tbClicks."timestamp" and dateadd(min, -10, tbClicks."timestamp")


select top 1000 * from adform.impressionsClicksMapped
where campaign_id = 929026
and publisher_domain=''
order by yyyy_mm_dd desc



--90391
select sum(matchingimpression) as totalMatched, count(*) as totals  from adform.impressions2Clicks


-- Get some matches
select top 1000 * from adform.impressions2Clicks
--where matchingImpression = 1
order by yyyy_mm_dd desc


select top 100 * from adform.meta_campaigns
where campaignsname LIKE '%uk_17+q4_beamly_joop%'


select top 1000 * from adform.meta_campaigns
where type = 'Display'


SELECT count(*)
from adform.meta_campaigns_v2 as tbClicks
where campaign_id is null


from adform.impressions
