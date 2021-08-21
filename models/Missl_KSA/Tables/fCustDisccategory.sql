
select *,row_number() over (partition by user_id order by cnt_orders desc) disc_cat_rank,'MissL' Halo_Country
from(

select user_id,disc_cat_id,count(distinct order_item_id) cnt_orders,min(order_date) date_start,max(order_date) date_stop
from
(select distinct user_id,order_item_id,order_date,
        case when round((SAFE_DIVIDE(item_discount,product_price))*100) =0 THEN 'no_promo'
             when round((SAFE_DIVIDE(item_discount,product_price))*100) <=30 THEN '0-30%'
             when round((SAFE_DIVIDE(item_discount,product_price))*100) <=50 THEN '30%-50%'
             ELSE '>50%' end as disc_cat
from `noted-computing-279322.halo_1_1.fOrders`
where order_status in ('successful') and total_item_price_wo_discount!=0) a,

`noted-computing-279322.halo_1_1.refCustDisccategory` b
where  a.disc_cat = b.disc_cat
group by user_id,disc_cat_id
)
