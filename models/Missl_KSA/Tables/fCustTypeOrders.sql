
select date_start,sum(case when order_by_custtype='Active' then Orders else 0 end) as Active,
                  sum(case when order_by_custtype='Lapsed' then Orders else 0 end) as Lapsed,
                  sum(case when order_by_custtype='Churned' then Orders else 0 end) as Churned,
                  'MissL' Halo_Country
from
(
select date_start,order_by_custtype,count(distinct order_id) Orders
from
(
select *, case when date_diff(date_start,Previous_order_date,Day)<=270 then 'Active'
              when date_diff(date_start,Previous_order_date,Day)<=450 then 'Lapsed'
              else 'Churned' end Order_By_CustType
from(
select distinct order_id,user_id,order_date date_start,lag(order_date,1) over (partition by user_id order by order_date) Previous_order_date
from `noted-computing-279322.halo_1_1.fOrders`
where order_status in ('successful')
)
)
group by 1,2
)
group by 1
