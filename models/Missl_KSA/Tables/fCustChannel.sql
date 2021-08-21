
select *,row_number() over (partition by user_id order by orders desc,revenue desc) channel_rank,'MissL' Halo_Country
from(
(
select user_id,channel_id,
count(distinct order_id) orders,
sum(total_item_price) Revenue,
min(order_date) date_start,
max(order_date) date_stop
from `noted-computing-279322.halo_1_1.fOrders` ord,
`noted-computing-279322.halo_1_1.refKeywords` refKey,
`noted-computing-279322.halo_1_1.refChannels` ref
where ord.ad_cat_id = refkey.ad_cat_id
and refkey.source_medium = ref.channel
group by user_id,channel_id))
