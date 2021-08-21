SELECT * except(row_number)
FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY entity_id order by _daton_batch_runtime desc ) row_number
from `noted-computing-279322.MissL1.magento2_missl_live_sales_order`)
WHERE row_number = 1 and store_id in (4,21)