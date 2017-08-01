
    -- Table keeping the 15 most frequent domains per campaign (CLICKS)
    CREATE TABLE adform.publisherClicks AS
    ( SELECT DISTINCT * FROM
        (
        SELECT
        --
        tbClicks.campaign_id,
        tbClicks.publisher_domain,
        count(*) AS numTotals,
        row_number() over  (partition by campaign_id
        order by numTotals desc) as rnk
        from adform.clicks as tbClicks
        group by 1,2
      ) AS A
      where A.rnk <= 15
      )


SELECT top 100 * from adform.publisherClicks
order by campaign_id, rnk ASC





    -- Table keeping the 15 most frequent domains per campaign (CLICKS)
    CREATE TABLE adform.publisherImpresssions AS
    ( SELECT DISTINCT * FROM
        (
        SELECT
        --
        tbClicks.campaign_id,
        tbClicks.publisher_domain,
        count(*) AS numTotals,
        row_number() over  (partition by campaign_id
        order by numTotals desc) as rnk
        from adform.impressions as tbClicks
        group by 1,2
      ) AS A
      where A.rnk <= 15
      )


  SELECT top 100 * from adform.publisherImpresssions
order by campaign_id, rnk ASC


SELECT top 100 * from adform.publisherImpresssions
where campaign_id = 875443
order by rnk ASC

SELECT top 100 * from adform.publisherClicks
where campaign_id = 875443
order by rnk ASC

-- Find some sort of relationship between impressions and clicks for a campaign, for one day
SELECT * FROM
adform.clicks
where campaign_id = 875443
and yyyy_mm_dd = '2017-06-22'
order by "timestamp" ASC


SELECT * FROM
adform.impressions
where campaign_id = 875443
and yyyy_mm_dd = '2017-06-22'
order by "timestamp" ASC



select dateadd(month,18, (select timestamp '2017-06-22 12:21:13'))
select dateadd(min,2, (select timestamp '2017-06-22  00:00:07'))


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