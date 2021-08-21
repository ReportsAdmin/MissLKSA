
    SELECT * except(row_number)
FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY value_id order by _daton_batch_runtime desc) row_number
FROM `noted-computing-279322.MissL1.magento2_missl_live_catalog_product_entity_varchar` )
WHERE row_number = 1 and store_id in (0,4,21)