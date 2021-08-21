
select lower(user_id) user_id,
count(distinct order_id) Orders,
sum(total_item_price) Revenue,
min(order_date) First_pur_date,
       max(order_date) last_pur_date,
       'MissL' Halo_Country
from `noted-computing-279322.halo_1_1.fOrders`
where order_status in ('successful') and user_id is not null
group by user_id