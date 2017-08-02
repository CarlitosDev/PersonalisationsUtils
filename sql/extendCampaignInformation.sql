select *
from adform.meta_campaigns
where (len(campaignsname)- len(replace(campaignsname, '_', ''))) >= 8


select campaignsid, campaignsname
from adform.meta_campaigns
where (len(campaignsname)- len(replace(campaignsname, '_', ''))) >= 8
and campaignsname like '%us_17+q4%'
order by len(campaignsname) desc


-- clean the bad records
update adform.meta_campaigns
set campaignsname = 'ar_17+q4_beamly_MJ_Daisy-may-june_xxxxxx_xxxxxx_awa_coty'
where campaignsname = 'cl_17+q4_beamly_MJ_Daisy-may-june_xxxxxx_xxxxxx_awa_coty_v2'

update adform.meta_campaigns
set campaignsname = 'us_17+q4_zenith_ClairolProfessional_CPFlareMe2H_XXXX_70378_AWA_COTY'
where campaignsname = 'us_17+q4_zenith_none_US_2017+4_Zenith_ClairolProfessional_CPFlareMe2H_70378_AWA_COTY_xxxxx_none_none_coty'

update adform.meta_campaigns
set campaignsname = 'us_17+q4_zenith_ClairolProfessional_CPFlareMe2H_xxxx_70378_AWA_COTY'
where campaignsname = 'us_17+q4_zenith__US_2017+4_Zenith_ClairolProfessional_CPFlareMe2H_70378_AWA_COTY_xxxxx___coty'

update adform.meta_campaigns
set campaignsname = 'us_17+q4_zenith_sally hansen_Sally Hansen Color Therapy GP_xxxxx_xxxx_engage_coty'
where campaignsid  = 892510

update adform.meta_campaigns
set campaignsname = 'us_17+q4_beamly_Sally Hansen_CSM_none_none__coty'
where campaignsid = 886811



create table adform.ammendedNames
AS (
select DISTINCT
campaignsid
,replace(replace(campaignsname, '___', '_none_none_'), '__', '_none_') as betterName
,-len(campaignsname) + len(replace(replace(campaignsname, '___', '_none_none_'), '__', '_none_')) as numChars
from adform.meta_campaigns
)

delete adform.ammendedNames where numchars = 0



select * from adform.ammendedNames
where numchars <> 0


select * from adform.meta_campaigns
where campaignsid = 897248


update sales
set qtysold = stagesales.qtysold,
pricepaid = stagesales.pricepaid
from stagesales
where sales.salesid = stagesales.salesid
and sales.listid = stagesales.listid
and stagesales.saletime > '2008-11-30'
and (sales.qtysold != stagesales.qtysold
or sales.pricepaid != stagesales.pricepaid);


create table adform.meta_campaigns_cp as
(select * from adform.meta_campaigns)



alter table adform.meta_ext_campaign rename to adform.meta_ext_campaign2
alter table adform.meta_ext_campaign rename to adform.meta_ext_cmp2
create table adform.meta_ext_2 AS (select * from adform.meta_ext_campaign)
--drop TABLE adform.meta_ext_campaign


select * from adform.meta_ext_campaign

-- Fix campaignobjective

select campaignobjective, count(*) from adform.meta_ext_campaign
group by 1
order by 2 desc


update adform.meta_ext_campaign
set campaignobjective = 'engagement'
where campaignobjective LIKE 'eng'

update adform.meta_ext_campaign
set campaignobjective = 'intent'
where campaignobjective LIKE 'intnt'

update adform.meta_ext_campaign
set campaignobjective = 'consideration'
where campaignobjective LIKE 'con'

update adform.meta_ext_campaign
set campaignobjective = 'engagement'
where campaignobjective LIKE 'eng'


update adform.meta_ext_campaign
set campaignobjective = 'awareness'
where campaignobjective LIKE 'Awareness'

update adform.meta_ext_campaign
set campaignobjective = 'purchase intent'
where campaignobjective LIKE 'pi'


update adform.meta_ext_campaign
set campaignobjective = 'consideration'
where campaignobjective LIKE 'CON'

update adform.meta_ext_campaign
set campaignobjective = 'website traffic'
where campaignobjective LIKE 'traffic'

update adform.meta_ext_campaign
set campaignobjective = 'purchase intent'
where campaignobjective LIKE 'intent'

update adform.meta_ext_campaign
set campaignobjective = 'purchase intent'
where campaignobjective LIKE 'pi'

----------

select * from adform.meta_ext_campaign

select bioniccampaignid, count(*) from adform.meta_ext_campaign
group by 1
order by 2 desc


-- Fix BionicId

update adform.meta_ext_campaign
set  bioniccampaignid = NULL
where bioniccampaignid LIKE '%xx'

update adform.meta_ext_campaign
set  bioniccampaignid = NULL
where bioniccampaignid ~ '\\D'

-- Select any non digit character
select bioniccampaignid
from adform.meta_ext_campaign
where bioniccampaignid ~ '\\D'

update adform.meta_ext_campaign
set  bioniccampaignid = NULL
where bioniccampaignid in (23432133,3803426, 3732614, 3737787)


-- Fix BionicId

update adform.meta_ext_campaign
set  bioniccampaignid = NULL
where bioniccampaignid LIKE '%xx'

-- Fix year

update adform.meta_ext_campaign
set  year = 17
where year ~ '\\D'

update adform.meta_ext_campaign
set  year = 17
where year = 2017


select year, count(*) from adform.meta_ext_campaign
group by 1
order by 2 desc

-- fix Quarter
select quarter, count(*) from adform.meta_ext_campaign
group by 1
order by 2 desc


update adform.meta_ext_campaign
set  quarter = 'q4'
where quarter in ('Q4', '04')



-- fix market
select market, count(*) from adform.meta_ext_campaign
group by 1
order by 2 desc


update adform.meta_ext_campaign
set  market = 'uk'
where market in ('UK')


update adform.meta_ext_campaign
set  market = 'nl'
where market in (' nl')

-------------

SELECT * from adform.meta_campaigns