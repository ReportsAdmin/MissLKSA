select *
from
(
select a.*,ifnull(overall_orders,0) overall_orders,
ifnull(overall_revenue,0) overall_revenue,
ifnull(overall_qty_sold,0) overall_qty_sold,
ifnull(successful_orders,0) successful_orders,
ifnull(successful_revenue,0) successful_revenue,
ifnull(successful_qty_sold,0) successful_qty_sold,
ifnull(overall_customers,0) overall_customers,
ifnull(successful_customers,0) successful_customers,
from
(
select distinct date,lower(trim(code)) code
from {{var('Calendar')}}
cross join
{{var('magento_affiliate_marketing')}}) a
left join
(
select lower(trim(code)) as coupon,order_date,count(distinct increment_id) as overall_orders,sum(total_item_price) as overall_revenue,
sum(product_quantity) as overall_qty_sold,
count(distinct case when order_status = 'successful' then increment_id end) as successful_orders,
sum(case when order_status = 'successful' then total_item_price end) as successful_revenue,
sum(case when order_status = 'successful' then product_quantity end) as successful_qty_sold,
count(distinct lower(user_id)) as overall_customers,
count(distinct case when order_status = 'successful' then lower(user_id) end) as successful_customers,
from {{var('magento_affiliate_marketing')}} x
left join `{{var('fOrders')}}` y
on lower(trim(x.code)) = lower(trim(y.coupon_code))
group by 1,2) b
on a.code = b.coupon
and a.date = b.order_date
) 
where overall_orders > 0