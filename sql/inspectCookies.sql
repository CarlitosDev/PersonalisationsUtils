-- List of top 5 cookies per campaign

    -- Table keeping the 5 most frequent cookies per campaign (CLICKS)
    CREATE TABLE adform.cookiesClicks AS
    ( SELECT DISTINCT * FROM
        (
        SELECT
        --
        tbClicks.campaign_id,
        tbClicks.cookie_id,
        count(*) AS numTotals,
        row_number() over  (partition by campaign_id
        order by numTotals desc) as rnk
        from adform.clicks as tbClicks
        group by 1,2
      ) AS A
      where A.rnk <= 5
      )


SELECT top 100 * from adform.cookiesClicks
order by campaign_id, rnk ASC



    -- Table keeping the 5 most frequent cookies per campaign (IMPRESSIONS)
    CREATE TABLE adform.cookiesImpressions AS
    ( SELECT DISTINCT * FROM
        (
        SELECT
        --
        tbClicks.campaign_id,
        tbClicks.cookie_id,
        count(*) AS numTotals,
        row_number() over  (partition by campaign_id
        order by numTotals desc) as rnk
        from adform.impressions as tbClicks
        group by 1,2
      ) AS A
      where A.rnk <= 5
      )


SELECT top 100 * from adform.cookiesImpressions
order by campaign_id, rnk ASC



SELECT * from adform.cookiesImpressions
where cookie_id in (SELECT DISTINCT cookie_id from adform.cookiesClicks)
order by campaign_id, rnk ASC



SELECT top 50 * from adform.impressions
where cookie_id = 5160093842341674315





-- Cookies from 'CLICKS' perspective
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

    from
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
        where cookie_id = -6521614284914147264
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14

    ) as tbClicks

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

-- Cookies from 'IMPRESISONS' perspective