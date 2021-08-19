
Select tp.* except(city), city.mapped_city__st as city from
(select ord.*, fulfil.time_to_fulfill, deliv.Delivered_quantity, deliv.Delivered_Orders, {{var('Country')}} Halo_Country from
(
--- Overall,SUccessful,cancelled orders
(Select
ifnull(trim(lower(city)),'unknown') as city, increment_id,
ifnull(count(distinct order_id),0) Overall_Orders,
ifnull(sum(total_item_price),0) Overall_Revenue,
ifnull(sum(product_quantity),0)  Overall_quantity,
ifnull(Sum(case when Order_status = 'successful' then total_item_price else null end ),0) Successful_revenue,
ifnull(Count (distinct(case when Order_status = 'successful' then order_id else null end )),0) Successful_orders,
ifnull(sum(case when Order_status = 'successful' then product_quantity else null end ),0) Successful_qty,
ifnull(Count (distinct(case when Order_status = 'canceled' then order_id else null end )),0) Cancelled_Orders,
ifnull(sum(case when Order_status = 'canceled' then product_quantity else null end ),0) Cancelled_quantity
from {{var('fOrders')}}
group by 1,2) ord



left join



---- fulfillement data
(select
 date_diff(delivered_date,order_date,day) time_to_fulfill , city, increment_id
 from (
 Select (case when trans.status like '%delivered%' then cast(trans.updated_at as date) end) delivered_date, ( case when  Order_status = 'successful' then ord.order_date end) order_date, trans.increment_id,ifnull(trim(lower(ord.city)),'unknown') as city
 from {{var('magento_transactionnew')}} trans
 left join  {{var('fOrders')}} ord
 on trans.increment_id = ord.increment_id
 ) group by time_to_fulfill,2,3) fulfil

 on ord.city = fulfil.city
 and ord.increment_id = fulfil.increment_id

 left join

 ---- delivered orders
 ( select  ifnull(trim(lower(address.city)),'unknown') as city, ord.increment_id,
sum (total_qty_ordered) as Delivered_quantity,ifnull(count(distinct ord.entity_id),0) Delivered_Orders
From {{var('magento_transactionnew')}} ord
left join {{var('magento_address')}} address
 on ord.entity_id=address.parent_id
Where status like '%delivered%'
group by 1,2

 ) deliv

 on ord.city = deliv.city
 and ord.increment_id = deliv.increment_id)) tp
left join
(select * from {{var('city_name')}}) city
on tp.city = city.current_city__st