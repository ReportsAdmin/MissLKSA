select *except(rownumber) from
(select distinct *,row_number() over(partition by product_id,category order by store_id desc) rownumber from
(select distinct cat.*,sub_cat.sub_category,sub_cat.sub_categoryid from
(select distinct *,case when level=2 then Value end as category,
case when level=2 then category_id end as categoryid
from
(select distinct category_id, product_id, b.value, c.value as prod_name, d.path, d.level,d.updated_at,b.store_id
from `noted-computing-279322.halo_1_1.magento_productcategoryproduct` a
left join `noted-computing-279322.halo_1_1.magento_productcategoryentityvarchar` b
on a.category_id=b.entity_id
left join `noted-computing-279322.halo_1_1.magento_productentityvarchar` c
on a.product_id=c.entity_id and c.store_id  = b.store_id
left join `noted-computing-279322.halo_1_1.magento_productcategoryentity` d
on a.category_id=d.entity_id
where b.attribute_id=42 and c.attribute_id=70)) cat
left join
(select distinct *,case when level=3 then Value end as sub_Category,
case when level=3 then category_id end as sub_Categoryid
from
(select distinct category_id, product_id, b.value, c.value as prod_name, d.path, d.level,b.store_id
from `noted-computing-279322.halo_1_1.magento_productcategoryproduct` a
left join `noted-computing-279322.halo_1_1.magento_productcategoryentityvarchar` b
on a.category_id=b.entity_id
left join `noted-computing-279322.halo_1_1.magento_productentityvarchar` c
on a.product_id=c.entity_id and c.store_id  = b.store_id
left join `noted-computing-279322.halo_1_1.magento_productcategoryentity` d
on a.category_id=d.entity_id
where b.attribute_id=42 and c.attribute_id=70)) sub_cat
on cat.product_id=sub_cat.product_id
and cat.store_id = sub_cat.store_id)
)where rownumber = 1 and level = 2