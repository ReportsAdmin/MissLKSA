
select * from (
select * ,'MissL' Halo_Country
from(
select
a.date_start,
a.product_id,
a.product_sku,
cast(c.M_ga_productListViews as float64) impressions,
cast(c.M_ga_productListClicks as float64) clicks,
cast(c.M_ga_productDetailViews as float64) productdetail_views,
cast(c.M_ga_productAddsToCart as float64) Addstocart,
b.orders,
b.units,
b.revenue
from
(
    select product_id, product_sku,date date_start
    from `noted-computing-279322.halo_1_1.Calendar` a,(select distinct product_id,product_sku,launch_date from `noted-computing-279322.halo_1_1.refProducts`) b
    where cast(a.date as date) >= b.launch_date
)  a
left join
(
select
 product_id,product_sku,
order_date,
count(distinct order_id) orders,
sum(product_quantity) units,
sum(total_item_price) revenue
from `noted-computing-279322.halo_1_1.fOrders`
where order_status in ('successful')
group by product_id,product_sku,order_date
) b
on a.product_id = b.product_id
and a.date_start = b.order_date
left join
`noted-computing-279322.halo_1_1.fGABaseSKU` c
on cast(c.D_ga_productSku as string)=cast(a.Product_id as string)
and PARSE_DATE('%Y%m%d', D_ga_date)= a.date_start
)
where product_id is not null

) where date_start>='2018-01-1'