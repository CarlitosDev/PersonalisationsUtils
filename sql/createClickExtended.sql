
/*

  Table extending clicks with the definitions from the 'meta' files
  ----------------

  The following temporal tables will be created in the process:
    - adform.clicksBreakDown (1941829 records)
    - adform.clicksBreakDown2
    - adform.clicksExtended (1927791 records)

  TO-DO:
    Match cityId to the meta_geo table - that is taking ages to upload-



*/

CREATE TABLE adform.clickTemp1 AS
  (
        SELECT
          yyyy_mm_dd,
          cast(tag_id as integer) as tag_id,
          cast(banner_id_ad_group_id as integer) as bannerid,
          cast(click_detail_id_paid_keyword_id as integer) as click_detail_id_paid_keyword_id,
          publisher_domain,
          cast(campaign_id as INTEGER) as campaign_id,
          cast(placement_id_activity_id as integer) as placement_id_activity_id,
          cookies_enabled,
          destination_url,
          cast(device_type_id as integer) as device_type_id,
          cast(client_id as integer) as client_id,
          cast(city_id as integer) as city_id,
          cast(browser_id as integer) as browser_id,
          visibility1_flag,
          count(*) as numRecords
        from adform.clicks
        where is_robot = 'No'
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
    )

DROP TABLE adform.clickExtended

CREATE TABLE adform.clickExtended AS
  (
    SELECT DISTINCT
    --
    tbClicks.campaign_id,
    tableB.campaignsname,
    tableB.startdate,
    tableB.enddate,
    tableB.type,
    --
    tbClicks.yyyy_mm_dd,
    --
    tbClicks.click_detail_id_paid_keyword_id,
    --
    tbClicks.cookies_enabled,
    tbClicks.publisher_domain,
    tbClicks.destination_url,
    --
    tbClicks.visibility1_flag,
    --
    tbClicks.bannerid,
    tbBanner.bannerSize,
    tbBanner.bannerType,
    tbBanner.deleted as bannerDeleted,
    tbBanner.BannerName,
    tbBanner.videoDuration,
    --
    tbClicks.browser_id,
    tbBrowsers.BrowserName,
    --
    tbClicks.client_id,
    tableClients.clientsname,
    --
    tbClicks.device_type_id,
    tableDevices.devicename,
    --
    tbClicks.placement_id_activity_id,
    tablePlacements.placementsname,
    --
    tbClicks.tag_id,
    tableTags.tagname,
    --
    tbClicks.city_id,
    tableCity.cityname,
    tableCity.regionid,
    tableCity.regioncode,
    tableCity.countryid,
    tableCity.countryname,
    --
    tbClicks.numRecords

    from adform.clickTemp1 as tbClicks

    inner join adform.meta_campaigns as tableB
      on tbClicks.campaign_id = tableB.campaignsid

    inner join adform.meta_banners as tbBanner
      on tbClicks.bannerid = tbBanner.BannerId

    inner join adform.meta_browsers as tbBrowsers
      on tbClicks.browser_id = tbBrowsers.browserid

    inner join adform.meta_clients as tableClients
      on tbClicks.client_id = tableClients.clientsid

    inner join adform.meta_devices as tableDevices
      on tbClicks.device_type_id = tableDevices.devicetypeid

    inner join adform.meta_placements as tablePlacements
      on tbClicks.placement_id_activity_id = tablePlacements.placementsid

    inner join adform.meta_tags as tableTags
      on tbClicks.tag_id = tableTags.tagid

    left join adform.meta_mini_geo as tableCity
      on tbClicks.city_id = tableCity.city_id

)
DISTKEY(campaign_id),
COMPOUND SORTKEY (yyyy_mm_dd, campaign_id)


drop table adform.meta_mini_geo2;

create table adform.meta_mini_geo2 AS
(SELECT DISTINCT * FROM adform.meta_mini_geo
order by cityname )


SELECT COUNT(*) FROM adform.meta_mini_geo
SELECT COUNT(*) FROM adform.meta_mini_geo

SELECT *  FROM adform.meta_mini_geo2
SELECT DISTINCT * FROM adform.meta_mini_geo
order by cityname


SELECT * from adform.clickExtended
where campaign_id = 886811
and  yyyy_mm_dd ='2017-06-16'
order by numrecords desc, yyyy_mm_dd ASC


SELECT count(*) from adform.clickExtended
where campaign_id = 886811
and  yyyy_mm_dd ='2017-06-16'




select top 1000 * from adform.clicksBreakDown
SELECT count(*) as numRecords from adform.clicksBreakDown
SELECT count(*) as numRecords from adform.clicksExtended

select top 1000 * from adform.clicksExtended
DROP TABLE adform.clicksBreakDown;
DROP TABLE adform.clicksBreakDown2;

--DROP TABLE adform.missingCities

-- deal with missing cities
create table adform.missingCities
AS (
select DISTINCT city_id from adform.clickExtended WHERE cityname is null
)

SELECT top 100 * from adform.missingCities

select count(DISTINCT(city_id)) from adform.clickExtended WHERE cityname is null


SELECT
campaign_id,
clientsname,
countryname,
devicename,
sum(numRecords) as totalClicks
from adform.clickExtended
group by 1,2,3,4
order by 1,2,totalclicks DESC



SELECT
A.campaign_id,
A.clientsname,
A.countryname,
A.devicename,
A.startdate,
A.enddate,
sum(A.numRecords) as totalClicks,
B.numTotals
from adform.clickExtended as A
inner join (
  SELECT  campaign_id,
  sum(numRecords) AS numTotals,
  rank() over (order by numTotals desc) as rnk
  from adform.clickExtended
  group by 1
) as B
on A.campaign_id = B.campaign_id
where B.rnk < 10
group by 1,2,3,4,5,6,B.numTotals
order by 1,2,totalclicks DESC


select count(*) from adform.meta_mini_geo


  SELECT  campaign_id,
  sum(numRecords) AS numTotals,
  row_number() over (
    partition by campaign_id
    order by numTotals DESC)
  from adform.clickExtended



  SELECT  campaign_id,
  sum(numRecords) AS numTotals,
  rank() over (order by numTotals desc) as rnk
  from adform.clickExtended
  group by 1



drop table adform.meta_mini_geo2;

create table adform.meta_mini_geo AS
(SELECT DISTINCT * FROM adform.meta_mini_geo2
order by cityname )


SELECT DISTINCT * FROM  adform.meta_mini_geo;


SELECT yyyy_mm_dd, count(*) as numRecords
from adform.clickExtended
group by 1
order by 1 desc


SELECT
A.campaign_id,
A.clientsname,
A.countryname,
A.devicename,
sum(A.numRecords) as totalClicks
from adform.clickExtended as A
where A.campaign_id = 886811
group by 1,2,3,4
order by totalClicks DESC
