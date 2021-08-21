select Inventory,productid,null Sizes,Selling_Price,null Discount,
     Case when selling_price>=100 and selling_price<10000 then "0 to 11k"
          when selling_price>=10000 and selling_price<=15000 then "11K to 15k"
          when selling_price>15000 and selling_price<=25000 then "15K to 25k"
          when selling_price>25000 and selling_price<=35000 then "25k to 35k"
          when selling_price>35000 and selling_price<=50000 then "25k to 35k"
          when selling_price>50000 then "Above 50k"
          else "other" End as Selling_Price_Category ,
          'MissL' Halo_Country
          ,MRP
from(
select * ,row_number() over (partition by productid order by  Selling_Price desc) ranki from(
select cast(Inventory as float64) Inventory,	cast(parent_id as string) as productid,	cast(Selling_Price as Float64) Selling_Price,
cast(MRP as float64) MRP



from(
select base1.*,base_price.base_price Selling_Price
 ,mrp.base_mrp MRP
from
(select entity.entity_id as parent_id,inv.qty Inventory  from

(select distinct entity_id from `noted-computing-279322.halo_1_1.magento_productentityint` ) entity
left join
(select distinct product_id,quantity as qty from `noted-computing-279322.halo_1_1.magento_stock_status` where quantity>0)inv
on entity.entity_id=inv.product_id) base1

left join

(select distinct  entity_id,value as base_mrp from `noted-computing-279322.halo_1_1.magento_productentitydec` where attribute_id= 74   ) mrp  -- 75

on base1.parent_id=mrp.entity_id
left join

(select distinct  entity_id, max(value ) base_price from `noted-computing-279322.halo_1_1.magento_productentitydec`
where attribute_id = 75 --411
group by 1 ) base_price

on base1.parent_id=base_price.entity_id)
)) where ranki = 1