

Select stock.product_id, stock.website_id, stock.stock_id, stock.qty as quantity, item.notify_stock_qty, prod.sku from
((SELECT * except(row_number)
FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY product_id order by _daton_batch_runtime desc) row_number
FROM `noted-computing-279322.MissL1.magento2_missl_live_cataloginventory_stock_status`)
WHERE row_number = 1) stock
join
(SELECT * except(row_number)
FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY product_id order by _daton_batch_runtime desc) row_number
FROM `noted-computing-279322.MissL1.magento2_missl_live_cataloginventory_stock_item`)
WHERE row_number = 1)item
on stock.product_id = item.product_id
join
(SELECT * except(row_number)
FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY entity_id order by _daton_batch_runtime desc) row_number
FROM `noted-computing-279322.MissL1.magento2_missl_live_catalog_product_entity`)
WHERE row_number = 1)prod
on stock.product_id = prod.entity_id)