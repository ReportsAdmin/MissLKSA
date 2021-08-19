select *,case when source_medium in {{AdSource}}  then 'Paid' else 'UnPaid' end Paid_NonPaid
from (
select *,{{CustomChannelGrouping}} CustomChannelGrouping
from (
select *,
--            case when Type in ('Direct','Offline','Organic','Referral','others') then 'UnPaid'
--               else 'Paid' end Paid_NonPaid,

              {{Country}} Halo_Country,
              split(source_medium ,'/')[safe_ordinal(1)] ChannelSource,
              split(source_medium ,'/')[safe_ordinal(2)] ChannelMedium,
              CASE When is_google_ad_source=true then 'GoogleCampaign'
                   when is_facebook_ad_source=true then 'FacebookCampaign'
                   ELSE "other"
                END campaign_grouping
from
(
select *,case when Publisher in ('Direct') then 'Direct'
              when Publisher in ('BibaOffline','Facebook_Social','narvar','OfflineFB','Offline') then 'Offline'
              when Publisher in ('Organic') then 'Organic'
              when Publisher in ('Referral') then 'Referral'
              when Publisher in ('Clickonik','Grabon','Admitad','omg','VCM','Vcommission','Affoy','Opicle','Catalyst','Adcanopus') then 'Affiliates'
              when Publisher in ('Icubes_Emailer','Icubes','Cartabandonement','MC','Netcore_SMS','Netcore_Email','Other_Email_SMS') then 'Emailer_SMS'
              when publisher in ('Facebook') then 'Facebook'
              when publisher in ('Google') then 'Google'
              when publisher in ('bing') then 'Bing'
              when publisher in ('Criteo') then 'Criteo'
              when publisher in ('Izooto' ) then 'Izooto'
              else 'others' end Type
from
(

select row_number() over() ad_cat_id,*,
       --cast(case when campaign_name is  null or campaign_name like '%not set%' then 0 else 1 end as boolean) is_ad_order,
       cast(case when source_medium in {{AdSource}} then 1 else 0 end as boolean) is_ad_order,
       cast(case when source_medium in {{GoogleAdSource}} then 1 else 0 end as boolean) is_google_ad_source,
       cast(case when source_medium in {{FacebookAdSource}} then 1 else 0 end as boolean) is_facebook_ad_source,
       case when source_medium like '%google%' or source_medium like '%Google%' then 'Google'
            when source_medium like '%facebook%' or source_medium like '%Facebook%' or source_medium like 'FB%'
             or source_medium like 'fb%'or source_medium like 'Fb%' or source_medium like '%social / cpc%' then 'Facebook'
            else 'Others' end Source,
        case when REGEXP_CONTAINS(source_medium,".*(?i)(direct).*") then 'Direct'
              when REGEXP_CONTAINS(source_medium,".*facebook / social.*") then 'Facebook_Social'
              when REGEXP_CONTAINS(source_medium,".*(?i)narvar.*") then 'narvar'
              when REGEXP_CONTAINS(source_medium,".*(?i)offlinefb.*") then 'OfflineFB'
              when REGEXP_CONTAINS(source_medium,".*(?i)offline.*") then 'Offline'
              when REGEXP_CONTAINS(source_medium,".*(?i)organic.*") then 'Organic'
              when REGEXP_CONTAINS(source_medium,".*(?i)referral.*") then 'Referral'
              when REGEXP_CONTAINS(source_medium,".*(?i)clickonik.*") then 'Clickonik'
              else 'Others'
              end Publisher
from (
select distinct *
from
(
{% if fb_array is defined and fb_array|length > 0 %}
select a.*,case when b.source_medium is null then {{FacebookCampaignChannel}} else b.source_medium end source_medium
from(
(
select  ad_name keyword,adset_name ad_content, campaign_name from `{{fFBBaseTable}}`

) a

left join

(select  D_ga_keyword keyword,D_ga_adContent ad_content, D_ga_campaign campaign_name, D_ga_sourceMedium source_medium
from `{{fGABaseCosts}}`) b

on a.keyword=b.{{keyword}} and a.ad_content=b.{{ad_content}} and a.campaign_name=b.campaign_name
)

union all
{% endif %}
select  D_ga_keyword keyword,D_ga_adContent ad_content, D_ga_campaign campaign_name, D_ga_sourceMedium source_medium
from `{{fGABaseCosts}}`)
)
)
)
)
)
