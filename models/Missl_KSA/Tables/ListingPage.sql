select *,
case when lower(D_ga_landingPagePath) like '%/necklaces%' then 'Necklaces' when lower(D_ga_landingPagePath) like '%/earings%' then 'Earrings'  when lower(D_ga_landingPagePath) like '%/rings%' then 'Rings' when lower(D_ga_landingPagePath) like '%/bracelets%' then 'Bracelets'
 when lower(D_ga_landingPagePath) like '%/anklets%' then 'Anklets' when lower(D_ga_landingPagePath) like '%/lady-fozaza%' then 'Lady Fozaza' when lower(D_ga_landingPagePath) like '%/shop-all%' then 'Shop All' when lower(D_ga_landingPagePath) like '%/offer%' then 'Offer' when lower(D_ga_landingPagePath) like '%/new-arrival%' then 'New Arrival' when lower(D_ga_landingPagePath) like '%/gif%' then 'Gift' else 'OTHERS'  end as cat,'MissL' Halo_Country from(
select 
sum(cast(M_ga_pageviews as int64)) as impressions,
D_ga_landingPagePath,
sum(cast(M_ga_productListClicks as int64)) as Clicks,
sum(cast(M_ga_productAddsToCart as int64)) addstocart,
safe_divide(sum(cast(M_ga_productAddsToCart as int64)),sum(cast(M_ga_productDetailViews as int64)))*100 as PvtoCart,
StartDate
from `noted-computing-279322.halo_1_1.fGABaseLandingPages`
where  lower(D_ga_landingPagePath) like '%/necklaces%' or lower(D_ga_landingPagePath) like '%/earrings%' or lower(D_ga_landingPagePath) like '%/rings%' or lower(D_ga_landingPagePath) like '%/bracelets%' or lower(D_ga_landingPagePath) like '%/anklets%' or lower(D_ga_landingPagePath) like '%/lady-fozaza%' or lower(D_ga_landingPagePath) like '%/shop-all%' or lower(D_ga_landingPagePath) like '%/offer%' or lower(D_ga_landingPagePath) like '%/new-arrival%' or lower(D_ga_landingPagePath) like '%/gif%'
group by StartDate,D_ga_landingPagePath)