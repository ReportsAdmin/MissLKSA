
select row_number() over() order_status_id,*,
      cast(case when order_status_detail in ('successful') then 1 else 0 end as boolean) Is_Successful_Order,
      'MissL' Halo_Country
from (
select orderstatus,
case orderstatus
--when 'closed'then 'closed'
when 'Returned' then 'Returned'
--when 'delivered' then 'delivered'
--when 'processing' then 'processing'
when 'canceled' then 'canceled'
--when 'payfort_fort_new' then 'payfort_fort_new'
when 'successful' then 'successful'
when 'Refunded' then 'Refunded'
else 'unsucessful'
end  as order_status_detail
from(
select distinct status orderstatus from  `noted-computing-279322.halo_1_1.magento_transaction`
)
)