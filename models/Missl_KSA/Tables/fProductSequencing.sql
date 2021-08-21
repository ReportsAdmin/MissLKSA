select *,row_number() over (partition by category_L1 order by RevPerImpression desc) DesiredPositioning,'MissL' Halo_Country
from(
select distinct * from (
select a.*,b.product_title,b.product_sku,b.category_L1,c.CatlogSequence,safe_divide(Revenue,Impressions) RevPerImpression
from
(
(select distinct product_id,sum(impressions) Impressions,sum(orders) Orders,sum(units) Units,sum(revenue) Revenue
from `noted-computing-279322.halo_1_1.fProductInsights`
where date_start between date_sub(current_date(),Interval 1 Month) and current_date()
group by 1) a

inner join

`noted-computing-279322.halo_1_1.refProducts` b
on  a.product_id=b.product_id

left  join

(select * from (
select distinct product_id,position CatlogSequence,row_number() over (partition by product_id order by position desc) rownumber
from `noted-computing-279322.MissL1.magento2_missl_live_catalog_category_product_index`
where is_parent=1
)
where rownumber=1) c
on a.product_id=cast(c.product_id as string)
)))
