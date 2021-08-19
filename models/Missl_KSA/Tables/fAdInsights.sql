
select *,{{Country}} Halo_Country
from (

{% if fb_array is defined and fb_array|length > 0 %}

select ad_cat_id, date_start,spend,Impressions, clicks
from
    (select cast(date_start as date) date_start,ad_name,adset_name,campaign_name,
            sum(cast(spend as float64)) spend,sum(cast(impressions as float64)) Impressions,sum(cast(clicks as float64)) clicks
     from `{{fFBBaseTable}}`
      group by 1,2,3,4) ins,

    (select * except(row_number) from(
     select *,row_number() over (partition by keyword, ad_content, campaign_name) row_number from `{{refKeywords}}`)
     where row_number=1) k

    where  k.keyword = ins.ad_name and k.ad_content=ins.adset_name and k.campaign_name = ins.campaign_name

     union all
{% endif %}

select ad_cat_id, cast(StartDate as date) date_start, cast(M_ga_adCost as float64)*{{ga_adpsend_exchangerate}} spend,
       cast(M_ga_impressions as float64) Impressions,cast(M_ga_adClicks as float64) clicks
from
  (select * except(M_ga_adCost,M_ga_impressions,M_ga_adClicks),
            case when D_ga_sourceMedium like '%acebook%' then '0' else M_ga_adCost end M_ga_adCost,
            case when D_ga_sourceMedium like '%acebook%' then '0' else M_ga_impressions end M_ga_impressions,
            case when D_ga_sourceMedium like '%acebook%' then '0' else M_ga_adClicks end M_ga_adClicks
  from `{{fGABaseCosts}}`
  where lower( D_ga_campaign ) like '%sa%' and lower( D_ga_campaign ) not like '%eg%'
  ) ins,

 `{{refKeywords}}` k

 where  k.keyword = ins.D_ga_keyword and k.ad_content= ins.D_ga_adContent and k.campaign_name = ins.D_ga_campaign and k.source_medium = ins.D_ga_sourceMedium and cast(M_ga_adCost as float64)>0
)