select *, from
(select prodinsights.*,stock.inventory,stock.mrp,stock.selling_price from (
(select prod.*,image.image from(
(select distinct * from (
select * , {{Country}} Halo_Country
from(
select
-- c.startdate date_start,
-- c.D_ga_productSku product_id,
a.order_date date_start,
a.product_id,
a.product_sku,
-- c.D_ga_productCategoryHierarchy as category_L2,
a.category category_L2,
a.sub_category as category_L1,
a.product_title,
-- c.D_ga_productName product_title,
cast(c.M_ga_productListViews as float64) impressions,
cast(c.M_ga_productListClicks as float64) clicks,
cast(c.M_ga_productDetailViews as float64) productdetail_views,
cast(c.M_ga_productAddsToCart as float64) Addstocart,
a.orders,
a.units,
a.revenue,
a.category_id,
a.Overall_revenue,
a.Overall_units,
a.Overall_orders
from
(select test.*,test2.category,test2.sub_category,test2.category_id from
(select
 product_id,
 product_sku,
 product_title,
 store_id,
order_date,
count(distinct order_id) Overall_orders,
sum(product_quantity) Overall_units,
sum(total_item_price) Overall_revenue,
sum(case when order_status in {{Authentic_Orders}} then total_item_price  end)  revenue,
Sum(case when order_status in {{Authentic_Orders}} then product_quantity end) units,
count(distinct case when order_status in {{Authentic_Orders}} then order_id end) orders
from `{{fOrders}}`
group by product_id,
product_sku,
product_title,
order_date,
store_id
    )test
    left join
(select distinct product_id, category, sub_category, prod_name,category_id,
  case when store_id = 0 then 4 else store_id end Cstore_id
 from `{{magento_missl_productscat}}`)test2
on test.product_id = cast(test2.product_id as string)
-- and test.product_title = test2.prod_name
and test.store_id = cast(test2.Cstore_id as STRING)
)a
left join
`{{fGABaseSKU}}` c
on cast(c.D_ga_productSku as string) = cast(a.Product_id as string)
--and a.category = c.D_ga_productCategoryHierarchy
--and a.product_title = c.D_ga_productName
and PARSE_DATE('%Y%m%d', D_ga_date)= a.order_date
)
where product_id IS NOT NULL
) where date_start>='2018-01-1'
)prod
left join
(select distinct Product_id, Image from `{{fProductImage}}`)image
on prod.product_id = image.Product_id)
)prodinsights
left join
(select * from `{{fProductStock}}`)stock
on prodinsights.product_id = stock.productid) )