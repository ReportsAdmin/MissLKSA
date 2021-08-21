SELECT * except(row_number)
FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY parent_id order by _daton_batch_runtime desc) row_number
FROM `noted-computing-279322.MissL1.magento3_missl_live_sales_order_address`)
WHERE row_number = 1