
select date(tr.created_at) order_date,cast(trd.Product_ID as string) Product_id,trd.Product_ID ChildProductId,split(trd.sku ,'-')[safe_ordinal(2)] Size,
       count(distinct trd.order_id) Orders,
       sum(trd.base_row_total) as total_item_price,
       sum(qty_ordered) Units,
       'MissL' Halo_Country
from
`noted-computing-279322.halo_1_1.magento_transaction_details` trd
left join
`noted-computing-279322.halo_1_1.magento_transaction` tr
on tr.entity_id = trd.order_id
where tr.status in ('successful')
group by 1,2,3,4
