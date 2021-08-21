    SELECT * except(row_number)
FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY parent_id,child_id  order by _daton_batch_runtime desc) row_number
FROM `noted-computing-279322.MissL1.magento2_missl_live_catalog_product_relation`)
WHERE row_number = 1