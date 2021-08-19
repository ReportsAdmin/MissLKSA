
select *,case when source_medium in ('google / cpc','facebook / cpc','facebook / paid')  then 'Paid' else 'UnPaid' end Paid_NonPaid
from (
select *, case when REGEXP_CONTAINS(source_medium,'.*(?i)snapchat.*') and REGEXP_CONTAINS(source_medium,'.*(?i)paid.*|.*(?i)cpc.*') then 'Snapchat_paid'
when REGEXP_CONTAINS(source_medium,'.*(?i)snapchat.*') then 'Snapchat_Unpaid'
when REGEXP_CONTAINS(source_medium,'.*(?i)Facebook.*|.*(?i)fb.*') and REGEXP_CONTAINS(source_medium,'.*(?i)cpc.*|.*(?i)paid.*') then 'Facebook_Dynamic_Remarketing'
when REGEXP_CONTAINS(source_medium,'.*(?i)Facebook.*|.*(?i)fb.*') then 'Facebook_Unpaid'
when REGEXP_CONTAINS(source_medium,'.*(?i)Instagram.*|.*(?i)IG.*') and source_medium like '%Instagram%'then 'Instagram_Unpaid'
when REGEXP_CONTAINS(source_medium,'.*(?i)Instagram.*|.*(?i)IG.*') then 'Instagram_paid'
when REGEXP_CONTAINS(source_medium,'.*(?i)TikTok.*') and REGEXP_CONTAINS(source_medium,'.*(?i)paid.*|.*(?i)cpc.*') then 'TikTok_paid'
when REGEXP_CONTAINS(source_medium,'.*(?i)TikTok.*') then 'TikTok_Unpaid'
when REGEXP_CONTAINS(source_medium,'.*(?i)lazurde.*') or REGEXP_CONTAINS(source_medium,'.*(?i)payfort.*') or
REGEXP_CONTAINS(source_medium,'.*(?i)gurubhyo.*') or REGEXP_CONTAINS(source_medium,'.*(?i)missl.*') or
REGEXP_CONTAINS(source_medium,'.*(?i)analytics.google.*') or REGEXP_CONTAINS(source_medium,'.*(?i)hotjar.*') or
REGEXP_CONTAINS(source_medium,'.*(?i)roihunter.*') or REGEXP_CONTAINS(source_medium,'.*(?i)leanport.*') or
REGEXP_CONTAINS(source_medium,'.*(?i)zendesk.*') or REGEXP_CONTAINS(source_medium,'.*(?i)aramex.*') or
REGEXP_CONTAINS(source_medium,'.*(?i)salesforce.*') or REGEXP_CONTAINS(source_medium,'.*(?i)skype.*') or
REGEXP_CONTAINS(source_medium,'.*(?i)saras.*') or REGEXP_CONTAINS(source_medium,'.*(?i)optimize.google.*') or
REGEXP_CONTAINS(source_medium,'.*(?i)adwords.corp.*') or REGEXP_CONTAINS(source_medium,'.*(?i)ads.google.*') or
REGEXP_CONTAINS(source_medium,'.*(?i)googleads.g.doubleclick.*') or REGEXP_CONTAINS(source_medium,'.*(?i)fedex.*') then 'Internal_Referral'
when REGEXP_CONTAINS(source_medium,'.*(?i)pinterest.*') or REGEXP_CONTAINS(source_medium,'.*(?i)youtube.*') or REGEXP_CONTAINS(source_medium,'.*(?i)twitter.*') or REGEXP_CONTAINS(source_medium,'.*(?i)linkedin.*') or
REGEXP_CONTAINS(source_medium,'.*(?i)t.co*') then 'Other_Social'
when REGEXP_CONTAINS(source_medium,'.*(?i)google.*') and REGEXP_CONTAINS(source_medium,'.*(?i)cpc.*|.*(?i)paid.*') then 'Paid_Search'
when REGEXP_CONTAINS(source_medium,'.*(?i)email.*') or REGEXP_CONTAINS(source_medium,'.*(?i)mail.*') or
REGEXP_CONTAINS(source_medium,'.*(?i)sendinblue.*') or REGEXP_CONTAINS(source_medium,'.*(?i)outlook.*') then 'Email'
when REGEXP_CONTAINS(source_medium,'.*(?i)yahoo.*') or REGEXP_CONTAINS(source_medium,'.*(?i)ecosia.*') or
REGEXP_CONTAINS(source_medium,'.*(?i)google.*') or REGEXP_CONTAINS(source_medium,'.*(?i)bing.*') or
REGEXP_CONTAINS(source_medium,'.*(?i)ask.*') or REGEXP_CONTAINS(source_medium,'.*(?i)baidu.*') or
REGEXP_CONTAINS(source_medium,'.*(?i)qwant.*') or REGEXP_CONTAINS(source_medium,'.*(?i)duckduckgo.*') then 'Organic_Search'
when REGEXP_CONTAINS(source_medium,'.*(?i)referral.*') then 'Referral'
when REGEXP_CONTAINS(source_medium,'.*(?i)direct.*') then 'Direct'
when REGEXP_CONTAINS(source_medium,'.*(?i)Facebook.*|.*(?i)fb.*') and REGEXP_CONTAINS(source_medium,'.*(?i)paid.*|.*(?i)cpc.*') then 'Facebook_Inhouse_paid' else 'Others' end CustomChannelGrouping
from (
select *,
--            case when Type in ('Direct','Offline','Organic','Referral','others') then 'UnPaid'
--               else 'Paid' end Paid_NonPaid,
              'MissL' Halo_Country,
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
       cast(case when source_medium in ('google / cpc','facebook / cpc','facebook / paid') then 1 else 0 end as boolean) is_ad_order,
       cast(case when source_medium in ('google / cpc') then 1 else 0 end as boolean) is_google_ad_source,
       cast(case when source_medium in ('facebook / cpc','facebook / paid') then 1 else 0 end as boolean) is_facebook_ad_source,
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
select a.*,case when b.source_medium is null then ('facebook / cpc') else b.source_medium end source_medium
from(
(
select  ad_name keyword,adset_name ad_content, campaign_name from `noted-computing-279322.halo_1_1.fFBBaseTable`
) a
left join
(select  D_ga_keyword keyword,D_ga_adContent ad_content, D_ga_campaign campaign_name, D_ga_sourceMedium source_medium
from `noted-computing-279322.halo_1_1.fGABaseCosts`) b
on a.keyword=b.keyword and a.ad_content=b.ad_content and a.campaign_name=b.campaign_name
)
union all
select  D_ga_keyword keyword,D_ga_adContent ad_content, D_ga_campaign campaign_name, D_ga_sourceMedium source_medium
from `noted-computing-279322.halo_1_1.fGABaseCosts`)
)
)
)
)
)
