select *, {{Country}} Halo_Country
from (

(select e.* except(increment_id) ,
g.Address,
g.Delivered_quantity,
g.Cust_Name,
g.Phone_number
from

(Select Lower(user_id) user_id,
ifnull(trim(lower(city)),'unknown') as city,
order_date,
increment_id,
ifnull(count(distinct order_id),0) Overall_Orders,
ifnull(sum(total_item_price),0) Overall_Revenue,

ifnull(sum(product_quantity),0)  Overall_quantity,
ifnull(Sum(case when Order_status = 'successful' then total_item_price else null end ),0) Successful_revenue,
ifnull(Count (distinct(case when Order_status = 'successful' then order_id else null end )),0) Successful_orders,
ifnull(sum(case when Order_status = 'successful' then product_quantity else null end ),0) Successful_qty,
ifnull(Count (distinct(case when Order_status = 'canceled' then order_id else null end )),0) Cancelled_Orders,
ifnull(sum(case when Order_status = 'canceled' then product_quantity else null end ),0) Cancelled_quantity
from `{{fOrders}}`

group by 1,2,3,4) e

left join
(select ord.customer_email,ord.customer_gender as Gender,sum(case when status like '%delivered%' then total_qty_ordered else null end ) Delivered_quantity,  ifnull(trim(lower(address.city)),'unknown') as city, (Case When address.street is Null then
concat ( ifnull(trim(lower(address.city)),'unknown'),",",'Saudi',"-",address.postcode) else
concat (address.street,",", ifnull(trim(lower(address.city)),'unknown'),",",'Saudi',"-",address.postcode) end )Address, ord.increment_id,
concat( ord.customer_firstname," ",  ord.customer_lastname) as Cust_Name, address.telephone as Phone_number
From `{{magento_transactionnew}}` ord
left join `{{magento_address}}` address
 on ord.entity_id=address.parent_id


group by 1,2,4,5,6,7,8) g
on e.increment_id = g.increment_id
and e.city = g.city)
cross join
(Select Sum(orders)/Count(*) as purchase_freq, count(case when orders > 1 then 1 else null end)/count(*) as repeat_rate, 1- count(case when orders > 1 then 1 else null end)/count(*) as churn_rate from
(
Select user_id, count(distinct order_id) as orders, sum(total_item_price) as revenue from `{{fOrders}}`
where order_status = 'successful'
group by 1))
cross join
(select spend/unique_cust as CAV
from
((Select sum(spend) spend  from `{{fAdInsights}}`)
cross join
(select count (distinct user_id) unique_cust from `{{fOrders}}`
where order_status = 'successful')))
)

