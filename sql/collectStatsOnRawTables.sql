
-- List of the campaigns and their presence
create table adform.impressionsList
AS (
  SELECT  campaign_id,
  count(*) AS numRecords,
  sum(case when publisher_domain = '' then 1 else 0 end) as unknownPubl,
  rank() over (order by numRecords desc) as rnk
  from adform.impressions
  group by 1
)


-- List of the campaigns and their presence
create table adform.clicksList
AS (
  SELECT  campaign_id,
  count(*) AS numRecords,
  sum(case when publisher_domain = '' then 1 else 0 end) as unknownPubl,
  rank() over (order by numRecords desc) as rnk
  from adform.clicks
  group by 1
)



-- List of the campaigns and their presence
create table adform.eventsList
AS (
  SELECT  campaign_id,
  count(*) AS numRecords,
  rank() over (order by numRecords desc) as rnk
  from adform.events
  group by 1
)


-- List of the campaigns and their presence
create table adform.trackingList
AS (
  SELECT  campaign_id,
  count(*) AS numRecords,
  rank() over (order by numRecords desc) as rnk
  from adform.tracking_points
  group by 1
)


-----------

select DISTINCT
tableA.campaign_id as campaign_id_imp,
tableA.numrecords  as numrecords_imp,
tableA.unknownPubl as missingPublisher_imp,
100*numrecords_imp/NULLIF(tableA.unknownPubl,0) as percMP_imp,
tableA.rnk         as rnk_imp,


tableB.campaign_id as campaign_id_clc,
tableB.numrecords  as numrecords_clc,
tableB.unknownPubl as missingPublisher_clc,
100*numrecords_clc/NULLIF(tableB.unknownPubl,0) as percMP_clc,
100.0*numrecords_clc/numrecords_imp as impressToClickRatio,

tableB.rnk         as rnk_clc,

tableC.campaign_id as campaign_id_evt,
tableC.numrecords  as numrecords_evt,
tableC.rnk         as rnk_evt,

tableD.campaign_id as campaign_id_tck,
tableD.numrecords  as numrecords_tck,
tableD.rnk         as rnk_tck,

tableE.*

from adform.impressionsList as tableA

left JOIN adform.clicksList as tableB
on tableA.campaign_id = tableB.campaign_id

left JOIN adform.eventsList as tableC
on tableA.campaign_id = tableC.campaign_id

left JOIN adform.trackingList as tableD
on tableA.campaign_id = tableD.campaign_id

left join adform.meta_campaigns_v2 as tableE
on cast(tableA.campaign_id as INTEGER) = tableE.campaign_id

order by tableA.rnk asc





SELECT
* FROM
adform.meta_campaigns_cp
where campaignsid in (872680,	912615,	873482,	878251,	875262,	957227,	960432,	885997,	960451,
888329,	903883,	954411,	909049,	886731,	957213,	941630,	907564,	875358,
953839,	957250,	876558,	953531,	916899,	895677,	873992,	955386,	910486,
904229,	954172,	955323,	875350,	895864,	906815,	930086,	956399,	956987,
955792,	956538,	960436,	957093,	870004,	955010,	957064,	957241,	959207,
960446,	960456,	957034,	960464,	957048,	955859,	956562,	867278,	956370,
862905)

SELECT
campaignsid,
campaignsname,
replace(replace(campaignsname, '_', ','), '+', ',') as betterName
 FROM
adform.meta_campaigns
where campaignsid in (872680,	912615,	873482,	878251,	875262,	957227,	960432,	885997,	960451,
888329,	903883,	954411,	909049,	886731,	957213,	941630,	907564,	875358,
953839,	957250,	876558,	953531,	916899,	895677,	873992,	955386,	910486,
904229,	954172,	955323,	875350,	895864,	906815,	930086,	956399,	956987,
955792,	956538,	960436,	957093,	870004,	955010,	957064,	957241,	959207,
960446,	960456,	957034,	960464,	957048,	955859,	956562,	867278,	956370,
862905)




SELECT
* FROM
adform.meta_ext_campaign
where campaignsid = 897248
