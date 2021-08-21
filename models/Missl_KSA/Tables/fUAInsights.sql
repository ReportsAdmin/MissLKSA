select
parse_date("%Y%m%d",D_ga_date) date_start,
cast(a.M_ga_sessions as float64) sessions,
cast(a.M_ga_impressions as float64) Impressions,
cast(a.M_ga_adClicks as float64) Adclickss,
cast(a.M_ga_productAddsToCart as float64) Addtocarts,
cast(a.M_ga_avgPageLoadTime as float64) avgPageLoadTime ,
cast(a.M_ga_bounceRate as float64) bounceRate,
b.ad_cat_id,b.Paid_NonPaid,b.is_ad_order,
'MissL' Halo_Country
from
`noted-computing-279322.halo_1_1.fGABaseCosts` a,
`noted-computing-279322.halo_1_1.refKeywords` b
where a.D_ga_keyword=b.keyword
and a.D_ga_adContent=b.ad_content
and a.D_ga_campaign= b. campaign_name
and a.D_ga_sourceMedium=b.source_medium
