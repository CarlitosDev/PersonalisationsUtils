/*

    The merging of the campaign tables:
      - meta_campaigns (from uploadMetasToRedShift.py)
      - meta_ext_campaign (extendCampaignInformation.sql + playgroundGetCampaignFields.py)

*/

CREATE TABLE adform.meta_campaigns_v2
as
(
  SELECT DISTINCT
  tableA.campaignsid as campaign_id,
  tableA.campaignstatus as campaignstatus,
  tableA.type,
  tableB.year                  ,
  tableB.quarter               ,
  tableB.market                ,
  tableA.startdate,
  tableA.enddate,
  tableA.clientid as client_id,
  tableB.masterbrand           ,
  tableA.label3 as brandBis,
  tableB."brand+campaignevent" ,
  tableB.bioniccampaignid      ,
  tableB.agency                ,
  tableB.clientname            ,
  tableA.label1 as division,
  tableB.campaignobjective     ,
  tableA.campaignsname
  -- meta campaigns is been generated with uploadMetasToRedShift.py
  from adform.meta_campaigns as tableA
  -- meta_ext_campaign (extendCampaignInformation.sql + playgroundGetCampaignFields.py)
  inner join adform.meta_ext_campaign as tableB
  on tableA.campaignsname = tableB.campaignsname
)



INSERT INTO adform.meta_campaigns_v2

  SELECT DISTINCT
  tableA.campaignsid as campaign_id,
  tableA.campaignstatus as campaignstatus,
  tableA.type,
  tableB.year                  ,
  tableB.quarter               ,
  tableB.market                ,
  tableA.startdate,
  tableA.enddate,
  tableA.clientid as client_id,
  tableB.masterbrand           ,
  tableA.label3 as brandBis,
  tableB."brand+campaignevent" ,
  tableB.bioniccampaignid      ,
  tableB.agency                ,
  tableB.clientname            ,
  tableA.label1 as division,
  tableB.campaignobjective     ,
  tableA.campaignsname
  -- meta campaigns is been generated with uploadMetasToRedShift.py
  from adform.meta_campaigns as tableA
  -- DDD
  inner join adform.ammendedNames as tableC
  on tableA.campaignsid = tableC.campaignsid

  -- meta_ext_campaign (extendCampaignInformation.sql + playgroundGetCampaignFields.py)
  inner join adform.meta_ext_campaign as tableB
  on tableC.bettername = tableB.campaignsname

-- And the third time....

INSERT INTO adform.meta_campaigns_v2

  SELECT DISTINCT
  tableA.campaignsid as campaign_id,
  tableA.campaignstatus as campaignstatus,
  tableA.type,
  tableB.year                  ,
  tableB.quarter               ,
  tableB.market                ,
  tableA.startdate,
  tableA.enddate,
  tableA.clientid as client_id,
  tableB.masterbrand           ,
  tableA.label3 as brandBis,
  tableB."brand+campaignevent" ,
  tableB.bioniccampaignid      ,
  tableB.agency                ,
  tableB.clientname            ,
  tableA.label1 as division,
  tableB.campaignobjective     ,
  tableA.campaignsname
  -- meta campaigns is been generated with uploadMetasToRedShift.py
  from adform.meta_campaigns as tableA
  -- DDD
  inner join adform.meta_ext_temp as tableB
  on tableA.campaignsid = tableB.campaignid









SELECT DISTINCT *
FROM adform.meta_campaigns_v2
where lower(agency) like 'beamly';


update adform.meta_campaigns_v2
set agency = 'Beamly'
where lower(agency) like 'beamly';

update adform.meta_campaigns_v2
set agency = 'Zenith'
where lower(agency) like 'zenith';


/*

    FIXES

 */

update adform.meta_campaigns_v2
set campaignobjective = 'engagement'
where campaignobjective LIKE 'eng';

update adform.meta_campaigns_v2
set campaignobjective = 'intent'
where campaignobjective LIKE 'intnt';

update adform.meta_campaigns_v2
set campaignobjective = 'consideration'
where campaignobjective LIKE 'con';

update adform.meta_campaigns_v2
set campaignobjective = 'engagement'
where campaignobjective LIKE 'eng';


update adform.meta_campaigns_v2
set campaignobjective = 'awareness'
where campaignobjective LIKE 'Awareness';

update adform.meta_campaigns_v2
set campaignobjective = 'purchase intent'
where campaignobjective LIKE 'pi';


update adform.meta_campaigns_v2
set campaignobjective = 'consideration'
where campaignobjective LIKE 'CON';

update adform.meta_campaigns_v2
set campaignobjective = 'website traffic'
where campaignobjective LIKE 'traffic';

update adform.meta_campaigns_v2
set campaignobjective = 'purchase intent'
where campaignobjective LIKE 'intent';

update adform.meta_campaigns_v2
set campaignobjective = 'purchase intent'
where campaignobjective LIKE 'pi';

----------

select * from adform.meta_campaigns_v2

select bioniccampaignid, count(*) from adform.meta_campaigns_v2
group by 1
order by 2 desc


-- Fix BionicId

update adform.meta_campaigns_v2
set  bioniccampaignid = NULL
where bioniccampaignid LIKE '%xx';

update adform.meta_campaigns_v2
set  bioniccampaignid = NULL
where bioniccampaignid ~ '\\D';

-- Select any non digit character
select bioniccampaignid
from adform.meta_campaigns_v2
where bioniccampaignid ~ '\\D';

update adform.meta_campaigns_v2
set  bioniccampaignid = NULL
where bioniccampaignid in (23432133,3803426, 3732614, 3737787);


-- Fix BionicId

update adform.meta_campaigns_v2
set  bioniccampaignid = NULL
where bioniccampaignid LIKE '%xx';

-- Fix year

update adform.meta_campaigns_v2
set  year = 17
where year ~ '\\D';

update adform.meta_campaigns_v2
set  year = 17
where year = 2017;


-- fix Quarter
update adform.meta_campaigns_v2
set  quarter = 'q4'
where quarter in ('Q4', '04');



-- fix market
select market, count(*) from adform.meta_campaigns_v2
group by 1
order by 2 desc


update adform.meta_campaigns_v2
set  market = 'uk'
where market in ('UK');


update adform.meta_campaigns_v2
set  market = 'nl'
where market in (' nl');

--------

SELECT DISTINCT masterbrand
FROM adform.meta_campaigns_v2
order by 1

update adform.meta_campaigns_v2
set  masterbrand = 'Covergirl'
where masterbrand in ('COVERGIRL', 'CoverGirl', 'covergirl');


update adform.meta_campaigns_v2
set  masterbrand = 'Calvin Klein'
where masterbrand in ('CK', 'calvin klein', 'calvin-klein', 'calvinklein', 'ck all');


update adform.meta_campaigns_v2
set  masterbrand = 'Marc Jacobs'
where masterbrand in ('Marc-Jacobs', 'Marc Jacbos');


update adform.meta_campaigns_v2
set  masterbrand = 'Covergirl'
where masterbrand in ('COVERGIRL', 'CoverGirl');


update adform.meta_campaigns_v2
set  masterbrand = 'Adidas'
where masterbrand in ('adidas', 'addidas female');


update adform.meta_campaigns_v2
set  masterbrand = 'Bourjois'
where masterbrand in ('bourjois', 'bourjois range', 'bourjois rouge laque ', 'bourjois volume reveal');

update adform.meta_campaigns_v2
set  masterbrand = 'David Beckham'
where masterbrand in ('david beckham', 'david-beckham', 'davidbeckham');

update adform.meta_campaigns_v2
set  masterbrand = 'Hugo Boss'
where masterbrand LIKE 'hugo%';

update adform.meta_campaigns_v2
set  masterbrand = 'Sally Hansen'
where masterbrand LIKE 'sally%';

update adform.meta_campaigns_v2
set  masterbrand = 'Covergirl'
where masterbrand in ('COVERGIRL', 'CoverGirl');

update adform.meta_campaigns_v2
set  masterbrand = 'Covergirl'
where masterbrand in ('COVERGIRL', 'CoverGirl');


update adform.meta_campaigns_v2
set  masterbrand = 'Gucci'
where masterbrand LIKE 'gucci%';

update adform.meta_campaigns_v2
set  masterbrand = 'System Professionals'
where masterbrand LIKE 'system%';


update adform.meta_campaigns_v2
set  masterbrand = 'Wella'
where masterbrand LIKE 'wella%';


update adform.meta_campaigns_v2
set  masterbrand = 'Rimmel'
where masterbrand LIKE 'rimmel%';

update adform.meta_campaigns_v2
set  masterbrand = 'Lancaster'
where masterbrand LIKE 'lancas%';

-------


SELECT *
FROM adform.meta_campaigns_v2
order by 1