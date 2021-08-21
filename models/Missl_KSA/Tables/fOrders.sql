select *,CASE WHEN FARM_FINGERPRINT(lower(user_id)) > 0 THEN CONCAT("A",ABS(FARM_FINGERPRINT(lower(user_id)))) WHEN FARM_FINGERPRINT(lower(user_id)) < 0 THEN CONCAT("B",ABS(FARM_FINGERPRINT(lower(user_id))))
 END customer_reference_id,
case when rank() over(partition by user_id,Is_Successful_Order order by order_datetime) = 1 and order_status in ('successful') then 1 else 0 end  is_new_customer
from
(
(select * from (
(select distinct * from (
select ad_cat_id,checkout_type,lower(coupon_code)coupon_code,currency_iso,is_ad_order,is_guest_order,is_new_customer1,item_discount,increment_id,
    no_promo_perc_discount,    order_date,    order_datetime,    cast(order_id as string) order_id ,    cast(order_item_id as string) order_item_id,order_status,     Paid_NonPaid,
   perc_discount, product_id,    product_launch_days,   total_item_price_wo_discount,  cast(user_id as string) user_id,   variantid,
   cast(product_price as float64) product_price , cast(product_quantity as float64) product_quantity ,DeviceCategory,country Country, shipping_amnt,
   cast(shipping_price as float64) shipping_price ,cast(tax_amount as float64) tax_amount,varient_sku product_sku,Is_Successful_Order,
   'null' mktplace_name, Halo_Country,city,case when Halo_Country = 'KalkiIndia' then total_item_price*exchange_rate else total_item_price end as total_item_price,
   total_item_price Net_Revenue,null RefundRevenue,null WebPrice,cast(case when increment_id is null then 1 else 0 end as boolean) is_item_returned,
   'null' DeliveryOption,totalitempricenew,product_title,store_id
from(
select cast(product_id as string) product_id,variantid,'MissL' Halo_Country,
* except(product_id,variantid)
from(
select
st.*,
ga.ad_cat_id,
ga.is_ad_order,
ga.Paid_NonPaid,
ga.DeviceCategory,
ex.exchange_rate ,
country.country,
'null' is_discount, 'null' is_international
from (
select *,
    ((cast(base_discount_amount as float64)*safe_divide((cast(product_price as float64)*cast(product_quantity as int64)),cast(grand_base_total as float64)))) item_discount,
    SAFE_DIVIDE((-(cast(base_discount_amount as float64)*safe_divide((cast(product_price as float64)*cast(product_quantity as int64)),cast(grand_base_total as float64)))),product_price)*100 perc_discount,
    case when ((cast(base_discount_amount as float64)*safe_divide((cast(product_price as float64)*cast(product_quantity as int64)),cast(grand_base_total as float64)))) is null then 1 else 0 end no_promo_perc_discount,
    case when coupon_code is not null then total_item_price else 0 end as grand_total_per_item,
--     case when coupon_code is not null then total_item_price - ((cast(base_discount_amount as float64)*safe_divide((cast(product_price as float64)*cast(product_quantity as int64)),cast(grand_base_total as float64))))
--     end as total_item_price_wo_discount,
    case when coupon_code is not null then (total_item_price - (cast(base_discount_amount as float64))) end as total_item_price_wo_discount,
    (base_row_total_incl_tax) as totalitempricenew,
        cast(case when customer_id is null then 1 else 0 end as boolean)is_guest_order
from(
select * except(ranki),
      case when cast(base_mrp as float64)<=cast(product_price as float64) and product_launch_days <=30  then total_item_price else 0 end is_new_arrival,
      case when rank() over(partition by user_id,Is_Successful_Order order by order_datetime) = 1 and order_status in ('successful') then 1 else 0 end  is_new_customer1
from(
select *,
      (case when base_row_total_incl_tax > 0 then
      base_row_total_incl_tax-base_discount_amount_item +
      ((shipping_amt)/(qty_ordered_item))*product_quantity else 0 end)
      - (case when gift_card_amount <> 0 then (base_row_total_incl_tax/subtotal_incl_tax)*(-1*gift_card_amount) else 0 end)
       as total_item_price,
		(case when base_row_total_incl_tax > 0  then ((shipping_amt)/(qty_ordered_item))*product_quantity else 0 end) as shipping_amnt,
       (cast(base_grand_total as float64)+ cast(total_store_credit_amount as float64)) as Overall_Revenue
from (
select *,
       sum(cast(base_item_total as float64)) over(partition by order_id order by date(order_datetime)) as grand_base_total,
       base_shipping_amount as shipping_amt, case when base_row_total_incl_tax > 0 then total_qty_ordered1 else null end as qty_ordered
from(
select *,
      (cast(product_price as float64)*product_quantity)as base_item_total,date_diff(date(order_datetime),date(cast(launch_date as datetime)),day) product_launch_days,
       rank() over (partition by user_id order by order_datetime) as ranki
from(
select
distinct ord.entity_id as order_id,
os.order_status_detail order_status,
os.Is_Successful_Order,
ord.coupon_code,
ord.customer_id,
ord.base_discount_amount,
  ord.gift_card_amount,
cast(ord.base_grand_total as float64) as base_grand_total,
ord.increment_id,
lower(ord.customer_email) user_id,
safe_divide(ord.base_tax_amount,count(distinct item.item_id) over (partition by ord.entity_id)) as tax_amount,
ord.order_currency_code as currency_iso,
ord.subtotal_incl_tax,
cast(ord.total_qty_ordered as float64) total_qty_ordered1,
count(distinct item.item_id) over (partition by ord.entity_id) total_qty_ordered,
cast(ord.base_shipping_incl_tax as float64) base_shipping_amount,
cast(ord.store_id as string)store_id,
DATETIME( ord.created_at,"Asia/Riyadh") as order_datetime,
date((DATETIME( ord.created_at,"Asia/Riyadh"))) as order_date,
cast(ord.total_store_credit_amount as float64) as total_store_credit_amount,
ord.shipping_amount as shipping_price,
extract(month from((DATETIME(ord.created_at,"Asia/Riyadh")))) as month,
extract(year from((DATETIME(ord.created_at,"Asia/Riyadh")))) as year,
extract(date from ((DATETIME(ord.created_at,"Asia/Riyadh")))) as date,
item.item_id as order_item_id,
item.product_id ,
item.sku as varient_sku,
item.parent_item_id,
item.qty_ordered as product_quantity,
item.base_price as product_price,
item.name as product_title,
  item.base_discount_invoiced as base_discount_invoiced,
-- cast(item.store_id as string) store_id,
'null' as variantid,
item.created_at as launch_date,
null base_mrp,
--item.base_row_total,
cast(item.base_row_total as float64) base_row_total,
cast(item.base_row_total_incl_tax as float64) base_row_total_incl_tax,
cast(item.base_discount_amount as float64) base_discount_amount_item,
item1.qty_ordered_item,
pay.method as checkout_type,
-- pro.variantid,
-- pro.variantsku,
-- pro.product_sku,
-- pro.launch_date,
stathist.status status_history,
address.city,
address.country_id,
--
user_id.user_info as userid
from `noted-computing-279322.halo_1_1.magento_transaction` as ord
left join `noted-computing-279322.halo_1_1.magento_transaction_details` item
on ord.entity_id=item.order_id
left join `noted-computing-279322.halo_1_1.refOrderStatus` os
on ord.status=os.orderstatus
left join `noted-computing-279322.halo_1_1.magento_payment` pay
on ord.entity_id=pay.parent_id
left join (select order_id, sum(case when base_row_total_incl_tax > 0 then qty_ordered end) as qty_ordered_item from
`noted-computing-279322.halo_1_1.magento_transaction_details`
-- where order_id = 2694
group by 1
 )item1
on ord.entity_id=item1.order_id
-- left join
-- `noted-computing-279322.halo_1_1.refProducts` pro
-- on cast(item.product_id as string)=pro.product_id
left join
`noted-computing-279322.halo_1_1.refUsers` user_id
on ord.customer_email=user_id.user_info
left join
`noted-computing-279322.halo_1_1.magento_orderhist` stathist
on ord.entity_id=stathist.parent_id
left join
`noted-computing-279322.halo_1_1.magento_address` address
on ord.entity_id=address.parent_id
))))) where product_price>=0
) st
left join
`noted-computing-279322.halo_1_1.fGATransactions` ga
on st.increment_id= ga.order_id
left join
`noted-computing-279322.halo_1_1.refExchangerate` ex
on st.date =ex.date
left join
`noted-computing-279322.halo_1_1.CountryCodesDataset` country
on st.country_id=country.CountryCode
))))forders1
left join
(select distinct lower(code) as influencerscode from `noted-computing-279322.halo_1_1.magento_influencerscode` where Currency = 'SAR')coupon
on forders1.coupon_code=coupon.influencerscode)where order_date > '2020-05-05' )
union all
(select distinct cast(ad_cat_id as int64) ad_cat_id,checkout_type,coupon_code,currency_iso,
cast(is_ad_order as bool) is_ad_order,cast(is_guest_order as bool) is_guest_order,
cast(is_new_customer as int64) is_new_customer,cast(item_discount as float64) item_discount,
cast(increment_id as string) increment_id,cast(no_promo_perc_discount as int64) no_promo_perc_discount,
order_date,cast(order_datetime as datetime) order_datetime,cast(order_id as string) order_id,
order_item_id,order_status,Paid_NonPaid,cast(perc_discount as float64) perc_discount,
cast(product_id as string) product_id,cast(product_launch_days as int64) product_launch_days,
cast(total_item_price_wo_discount as float64) total_item_price_wo_discount,user_id,variantid,
cast(product_price as float64) product_price,cast(product_quantity as float64) product_quantity,DeviceCategory,
Country,0 as shipping_amnt, cast(shipping_price as float64) shipping_price,cast(tax_amount as float64) tax_amount,
cast(product_sku as string) product_sku,cast(Is_Successful_Order as bool) Is_Successful_Order,mktplace_name,Halo_country,
city,cast(total_item_price as float64) total_item_price,cast(Net_Revenue as float64) Net_Revenue,
cast(RefundRevenue as int64) RefundRevenue,cast(WebPrice as int64) WebPrice,cast(is_item_returned as bool) is_item_returned,
DeliveryOption,cast(totalitempricenew as float64) totalitempricenew,influencerscode,'null' as product_title,'null' as store_id
from `noted-computing-279322.halo_1_1.magento_historicorders`))
