select *except(rank) from (
select *,row_number() over(partition by product_id order by image desc) rank from (
select cast(entity_id as string) as Product_id,concat(value,'catalog/product',val) as Image,'MissL' Halo_Country from(
select * from
(select value,'1' joinvalue from  `noted-computing-279322.halo_1_1.magento_Image1`
where config_id=66 )a
left join
(select distinct entity_id,value as val,'1' jointvalue from `noted-computing-279322.halo_1_1.magento_productentityvarchar`
where attribute_id = 84)b
on a.joinvalue=b.jointvalue
left join
( select distinct value_id url_id,entity_id product_id1,position from  `noted-computing-279322.halo_1_1.magento_Image3`
where disabled = 0)c
on b.entity_id=c.product_id1
)
where position=1
)
order by product_id)
where rank = 1