select * from (
select pro.*except(product_title),ColLev.product_type CollectionLevel, cat.category as category_L2 ,cat.prod_name as product_title,
cat.sub_category as category_L1,Categoryid as category_L1_id
from(
(select a.color,a.fit,a.length1,a.material,a.neckline,
      'null' variantid,
      b.sku product_sku,
      product_title,
      size,
      sleevelength,
      style,
      variantId product_id,
     'null' variantSku,
      Img,
--       category_L1	,
--       category_L2,
      launch_date,
--       category_L1_id,
      Halo_Country
from
(
(select * except(launch_date),min(launch_date) over (partition by product_id) launch_date,
--     cast(case when category_L1= 'Diamond' then 1
--          when category_L1= 'Platinum' then 2
--          when category_L1='Gold' then 3
--          else 4
--          end  as string) category_L1_id,
         'MissL' Halo_Country
 from(
select color,	fit,	length1,	material,	neckline,	product_id,
product_sku,	product_title,	size,	sleevelength,	style,	variantId,'null' variantSku,launch as launch_date,'null' Img,
(case when material in("Gold and Diamonds","Gold and Gemstones","Gold, Diamond and Gemstones","Platinum and Diamond") then 'Diamond'
when material in("Only Platinum","Platinum and Swarovski Zirconia") then 'Platinum'
when material in("Only Gold","Gold , Swarovski Zirconia and Gemstones") then 'Gold' else 'Others' end) category_L1,category_L2
from(
select cast(product_id as string) product_id,cast(variantId as string) variantId,base2.* except(product_id,variantId),parsku.parent_sku as product_sku,col.color,launch.launch from(
(select * from(
select * except(parent_id),case when parent_id is null then variantid else parent_id end as product_id, null collection,null season,null category,null fabric from(
select base1.*,par.parent_id from
(select
pr.entity_id variantId,
max(case when attribute_code like 'name' then value end) category_L2,
max(case when attribute_code like 'name' then value end) product_title,
max(case when attribute_code like 'size' then value end) size,
max(case when attribute_code like 'length' then value end) length1,
max(case when attribute_code like 'sleeve' then value end) sleevelength,
max(case when attribute_code like 'style' then value end) style,
max(case when attribute_code like 'specific fit' then value end) fit,
max(case when attribute_code like 'material' then value end) material,
max(case when attribute_code like 'collar' then value end) neckline
from
(
select
attr.value_id,
attr.attribute_id,
attr.entity_id,
attr.value value
from
(select distinct * from
(
select value_id, attribute_id, entity_id, cast(value as string) value from `noted-computing-279322.halo_1_1.magento_productentityvarchar`
union all
select atr.value_id, atr.attribute_id, atr.entity_id, cast(opval.value as string) value
from
`noted-computing-279322.halo_1_1.magento_productentityint` atr
left join
`noted-computing-279322.halo_1_1.magento_producteavoption` op
on atr.attribute_id = op.attribute_id
and atr.value = op.option_id
left join
(select * from `noted-computing-279322.halo_1_1.magento_producteavoptionvalue` opval
where value_id = (select max(value_id)
from `noted-computing-279322.halo_1_1.magento_producteavoptionvalue` opval1 where
opval.option_id = opval1.option_id)
) opval
on op.option_id = opval.option_id
)) attr
) pr,
(select attribute_id, attribute_code
from `noted-computing-279322.halo_1_1.magento_producteav`
where attribute_code in ('key_category', 'key_subcategory','candere_product_type','brand_product','name','created_at','color','collar','size','length','sleeve','specific fit','style','material')) attr
where pr.attribute_id= attr.attribute_id
group by pr.entity_id)base1
left join
(select distinct parent_id,child_id from `noted-computing-279322.halo_1_1.magento_productrelation` ) par
on base1.variantid=par.child_id
) ))base2
left join
(select entity_id,sku as parent_sku from `noted-computing-279322.halo_1_1.magento_productentity`) parsku
on base2.product_id=parsku.entity_id
left join
(select entity_id,opval2.value as color from(
(select entity_id,value from `noted-computing-279322.halo_1_1.magento_productentitytext`
where attribute_id=374)color_id
left join
(select max(value) as value,option_id from `noted-computing-279322.halo_1_1.magento_producteavoptionvalue`
group by option_id) opval2
on cast(color_id.value as int64)=cast(opval2.option_id as int64))) col
on base2.variantid=col.entity_id
)
left join
(select min(date(created_at)) as launch,product_id as product_id1 from `noted-computing-279322.halo_1_1.magento_transaction_details`
group by product_id1)launch
on base2.variantid=launch.product_id1
)
)) a
left join
(select distinct product_id,sku
from `noted-computing-279322.halo_1_1.magento_transaction_details`
) b
on a.variantid=cast(b.product_id as string)
)) Pro
left join
 `noted-computing-279322.halo_1_1.magento_missl_productscat`  cat
on pro.product_id=cast(cat.product_id as string)
left join
(SELECT eav_attribute_option_value.value as product_type,catalog_product_entity_int.entity_id
FROM
`noted-computing-279322.halo_1_1.magento_productentityint` catalog_product_entity_int
     INNER JOIN
`noted-computing-279322.halo_1_1.magento_producteavoptionvalue`  eav_attribute_option_value
 ON  catalog_product_entity_int.value = eav_attribute_option_value.option_id
     WHERE  catalog_product_entity_int.attribute_id = 443) ColLev
on pro.product_id=cast(ColLev.entity_id as string)
)
)
where category_L2 is not null