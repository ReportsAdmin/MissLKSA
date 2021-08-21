
    SELECT * except(row_number)
FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY config_id order by _daton_batch_runtime desc) row_number
FROM `noted-computing-279322.MissL1.magento2_missl_live_core_config_data`)
WHERE row_number = 1