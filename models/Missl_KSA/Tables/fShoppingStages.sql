select date_start,keyword,adcontent,campaign,SourceMedium,Devicecategory,
sum(case when  ShoppingStage = "ALL_VISITS" then sessions else 0 end) as Allvisits,
sum(case when ShoppingStage = "PRODUCT_VIEW" then sessions else 0 end) as Productviews,
sum(case when  ShoppingStage = "ADD_TO_CART" then sessions else 0 end) as Addstocart,
sum(case when ShoppingStage = "CHECKOUT" then sessions else 0 end) as Checkout,
sum(case when  ShoppingStage = "CHECKOUT_1" then sessions else 0 end) as Shipping,
sum(case when ShoppingStage = "CHECKOUT_2" then sessions else 0 end) as PaymentOption,
sum(case when ShoppingStage = "CHECKOUT_3" then sessions else 0 end) as PaymentClick,
sum(case when  ShoppingStage = "CHECKOUT_1" then sessions else 0 end) as AddstocartPage,
sum(case when ShoppingStage = "CHECKOUT_2" then sessions else 0 end) as LogginPage,
sum(case when  ShoppingStage = "CHECKOUT_3" then sessions else 0 end) as ShippingPage,
sum(case when ShoppingStage = "CHECKOUT_3" then sessions else 0 end) as PaymentPage,
sum(case when ShoppingStage = "TRANSACTION" then sessions else 0 end) as Transactions,
'MissL' Halo_Country
from
(
select parse_date('%Y%m%d', D_ga_date)  date_start,D_ga_shoppingStage ShoppingStage,D_ga_keyword keyword,D_ga_adContent adcontent,D_ga_campaign Campaign,
       D_ga_sourceMedium SourceMedium,D_ga_deviceCategory Devicecategory,sum(cast(M_ga_sessions as int64)) as sessions
from `noted-computing-279322.halo_1_1.fGABaseShoppingStages`
group by 1,2,3,4,5,6,7
)
group by 1,2,3,4,5,6