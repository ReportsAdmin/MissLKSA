select  * from (
SELECT * except(row_number)
FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY D_ga_date,D_ga_productSku,D_ga_productCategoryHierarchy,D_ga_productName
                                                order by _daton_batch_runtime desc) row_number
FROM `noted-computing-279322.MissL1.halo_ga1_216655333_category_product1`)
WHERE row_number = 1
)