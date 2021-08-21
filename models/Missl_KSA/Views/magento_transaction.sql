select  0 total_store_credit_amount, 
       
       
       
       * from
(select ta.*except(status),
case when tb.status = 'successful' then 'successful'
when ta.status in ('canceled','holded') then 'canceled'
when ta.status = 'Refund_closed' then 'Refunded'
when lower(ta.status) like '%return%' then 'Returned' else 'unsucessful' end status
from
(SELECT * except(row_number)
FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY entity_id order by updated_at desc ) row_number
FROM `noted-computing-279322.MissL1.magento2_missl_live_sales_order`)
WHERE row_number = 1 and store_id in (4,21))ta

left join

(select distinct entity_id,customer_contacted,payfort_paid,'successful' status from
(select * from
(select * from
(SELECT * except(row_number)
FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY entity_id order by updated_at desc ) row_number
FROM `noted-computing-279322.MissL1.magento2_missl_live_sales_order`)
WHERE row_number = 1)
where status in ('complete','processing','payfort_fort_new','processing_confirmed','processing_shipped',
'closed','pending') or status like '%delivered%')
where store_id in (4,21))a
left join
(select distinct entity_id as entityid,increment_id as incrementid,customer_contacted,payfort_paid from
 `noted-computing-279322.MissL1.magento2_missl_live_sales_order_grid`) b
ON a.increment_id = b.incrementid
where lower(customer_contacted) = 'yes' or lower(payfort_paid) = 'yes')tb
on ta.entity_id= tb.entity_id)
