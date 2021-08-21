
select cal.Halo_Country,cal.date_start,cal.source_medium,a.* except(date_start,source_medium,Halo_Country),
       b.Spend,b.Impressions,b.Adclickss,c.Orders,c.Revenue
from
(


(select source_medium,date date_start,Halo_Country
 from `noted-computing-279322.halo_1_1.Calendar` a,(select distinct source_medium from `noted-computing-279322.halo_1_1.refKeywords` ) b
 ) cal
    
 left join

(select date_start,source_medium,Halo_Country,sum(sessions) sessions,
--         sum(Impressions) Impressions, sum(Adclickss) Adclickss,
        sum(Addtocarts) Addtocarts,sum(Bounces) Bounces
from (
select a.*,b.source_medium,(a.bounceRate * a.sessions)/100 Bounces
from `noted-computing-279322.halo_1_1.fUAInsights` a, `noted-computing-279322.halo_1_1.refKeywords` b
where a.ad_cat_id=b.ad_cat_id
)
group by 1,2,3) a
on cal.date_start=a.date_start and cal.source_medium=a.source_medium

left  join


(select date_start,source_medium,Halo_Country,sum(spend) spend,sum(Impressions) Impressions,sum(clicks) Adclickss
from(
select a.*,b.source_medium
from `noted-computing-279322.halo_1_1.fAdInsights` a, `noted-computing-279322.halo_1_1.refKeywords` b
where a.ad_cat_id=b.ad_cat_id
)
group by 1,2,3) b
on cal.date_start=b.date_start and cal.source_medium=b.source_medium

left join

(select order_date,source_medium,Halo_Country,count(distinct order_id) Orders,sum(total_item_price) Revenue
from(
select a.*,b.source_medium
from `noted-computing-279322.halo_1_1.fOrders` a, `noted-computing-279322.halo_1_1.refKeywords` b
where a.ad_cat_id=b.ad_cat_id
)
where is_successful_order=true
 
group by 1,2,3) c
on cal.date_start=c.order_date and cal.source_medium=c.source_medium
)
where cal.source_medium in ('google / cpc','facebook / cpc','facebook / paid')
