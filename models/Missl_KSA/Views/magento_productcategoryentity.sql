
    SELECT * except(row_number)
FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY entity_id order by Updated_at desc) row_number
FROM `noted-computing-279322.MissL1.magento2_missl_live_catalog_category_entity` )
WHERE row_number = 1
