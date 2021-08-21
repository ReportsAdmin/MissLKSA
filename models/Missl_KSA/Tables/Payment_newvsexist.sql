select method,date,count(distinct new_cust_pay)new_cust_pay,
count(distinct Exist_cust_pay)Exist_cust_pay,'MissL' Halo_Country from
(select method,date,case when ranki=1 then a.entity_id end as new_cust_pay,
case when ranki>1 then a.entity_id end as Exist_cust_pay
from
(select * from(
select *,rank() over (partition by customer_email order by date) ranki from (
select distinct entity_id,customer_email,date(datetime(created_at,'Asia/Riyadh')) date
 from `noted-computing-279322.halo_1_1.magento_transaction`
))
--where ranki=1
)a
left join
(select distinct parent_id,method from `noted-computing-279322.halo_1_1.magento_payment` )b
on a.entity_id=b.parent_id)
group by 1,2,5
order by date desc