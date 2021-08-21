select  0 total_store_credit_amount, 
       
       
       
       * from

(select distinct increment_id,date(datetime(created_at,'Asia/Riyadh'))created_date,date(datetime(updated_at,'Asia/Riyadh'))updated_date,
case when status like '%delivered%' then 'delivered'
when status = 'processing_shipped' then 'shipped'
else 'confirmed' end status
from
(select * from
(select * from
(SELECT *
FROM (SELECT *
FROM `noted-computing-279322.MissL1.magento2_missl_live_sales_order`))
where lower(status) in ('complete','processing','payfort_fort_new','processing_confirmed','processing_shipped',
'closed','pending') or lower(status) like '%delivered%')
where store_id in (4,21))a
left join
(select distinct increment_id as incrementid,customer_contacted,payfort_paid from
 `noted-computing-279322.MissL1.magento2_missl_live_sales_order_grid`) b
ON a.increment_id = b.incrementid
where lower(customer_contacted) = 'yes' or lower(payfort_paid) = 'yes')