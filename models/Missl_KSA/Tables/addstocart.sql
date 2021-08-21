select cal.date,Prodaddstocart,Navaddstocart,'MissL' Halo_Country from
(select date from `noted-computing-279322.halo_1_1.Calendar` )cal
left join
(select 
StartDate,
sum(cast(M_ga_productAddsToCart as int64)) Prodaddstocart
from `noted-computing-279322.halo_1_1.fGABaseLandingPages`
where   lower(D_ga_landingPagePath) like '%/product/view/%'
group by StartDate)a
on cal.date=a.StartDate
left join
(select
StartDate,
sum(cast(M_ga_productAddsToCart as int64)) Navaddstocart
from `noted-computing-279322.halo_1_1.fGABaseLandingPages`
where  lower(D_ga_landingPagePath) like '%/necklaces%' or lower(D_ga_landingPagePath) like '%/earrings%' or lower(D_ga_landingPagePath) like '%/rings%' or lower(D_ga_landingPagePath) like '%/bracelets%' or lower(D_ga_landingPagePath) like '%/anklets%' or lower(D_ga_landingPagePath) like '%/lady-fozaza%' or lower(D_ga_landingPagePath) like '%/shop-all%' or lower(D_ga_landingPagePath) like '%/offer%' or lower(D_ga_landingPagePath) like '%/new-arrival%' or lower(D_ga_landingPagePath) like '%/new-arrival%' or lower(D_ga_landingPagePath) like '%/gif%'
group by StartDate)b
on cal.date=b.StartDate
order by date desc