
select * except(date_level1) from (
(select * except(date_level1) from (
(select * except(date_level1) from (
(select * except(date_level1) from (
(select * except(date_level1) from (
(select * except(date_level1) from (
(select * except(date_level1) from (
(select * except(date_level1) from (
(select * except(date_level1) from (
(select * except(date_level1) from (
(select * except(date_level1) from (
(select * except(date_level1) from (
(select * except(date_level1) from (
(select * except(date_level1) from (
(select * except(date_level1) from (
(select * except(date_level1) from (
(select * except(date_level1) from (
(select * except(date_level1) from (
(select * except(date_level1) from (
(select * except(date_level) from (
(select * except(date_level) from (
(select * except(date_level) from (
(select * except(date_level) from (
(select * except(date_level) from (
(select * except(date_level) from
(select * except(date_level) from
(select * except(date_gender) from
(select * except(date_level) from
(select * except(date_level) from
(select * except(date_level) from (
(select
'MissL' Halo_Country,
a.date,
ua.sessions,
ad.spend,
store.total_product_quantity,
store.total_item_price,
store.total_ad_revenue,
store.total_item_discount,
store.per_discount,
store.orders,
store.users,
store.total_product_quantity_auth,
store.total_item_price_auth,
store.total_ad_revenue_auth,
store.total_non_ad_revenue_auth,
store.total_item_discount_auth,
store.per_discount_auth,
store.orders_auth,
store.users_auth,
store.New_Customers,
store.Existing_Customers,
store.New_Customer_Orders,
store.Existing_Customer_Orders,
store.New_Customer_Revenue,
store.Existing_Customer_Revenue,
store.GrossRevenue,
store.NetRevenue,
store.MSRP,
store.ReturnsRevenue,
store.ReturnedUnits,
store.PaidNetRevenue,
store.NonPaidNetRevenue,
store.Itemscancelled,
store.cancelled_orders,
store.cod,
store.cc,
store.tabby,
store.returned_orders,
store.tax_amount,
store.influencersrevenue,
store.Total_revenue,
store.unsucessful_revenue,
store.influencersorders,
store.unsucessful_orders,
ss.Allvisits,
ss.Productviews,
ss.Addstocart,
ss.Checkout,
ss.Shipping,
ss.PaymentOption,
ss.PaymentClick,
ss.Transactions,
ss.AddstocartPage,
ss.LogginPage,
ss.ShippingPage,
ss.PaymentPage,
store.Paid_orders,
ua.Paid_sessions,
store.Non_Paid_orders,
ua.Non_Paid_sessions,
us.visitors,
us.registered_users,
us.Bounce,
merc.DiscountedSales,
merc.NonDiscountedSales,
prod.Impressions,
prod.Clicks,
prod.ProductDetailViews,
prod.ProductAddstocart,
pg.avgSessionDuration,
pg.avgPageLoadTime,
pg.sessions as  PgSessions,
pg.exits,
-- LP_Lan.LP_deviceCategory,
-- LP_Lan.LP_browser,
LP_Lan.LP_Lan_Bounce,
LP_Lan.LP_Lan_Sessions,
LP_Lan.LP_Lan_Transactions,
-- H_Lan.H_deviceCategory,
-- H_Lan.H_browser,
H_Lan.H_Lan_Bounces,
H_Lan.H_Lan_Sessions,
H_Lan.H_Lan_transactions,
-- P_Lan.P_deviceCategory,
-- P_Lan.P_browser,
P_Lan.P_bounces,
P_Lan.P_Transactions,
P_Lan.P_Sessions,
Home_Pg.H_Sessions,
Home_Pg.H_AvgsesDur,
Home_Pg.H_Pages_Per_Sess,
Home_Pg.H_AvgPgLT,
Home_Pg.H_Bounces,
Home_Pg.H_Transactions,
List_pg.impressions LP_impressions,
List_pg.Clicks LP_Clicks,
List_pg.addstocart LP_addstocart,
List_pg.productDetailViews LP_productDetailViews
from
`noted-computing-279322.halo_1_1.Calendar` a
left join
(select date_start, sum(spend) spend  from `noted-computing-279322.halo_1_1.fAdInsights` group by date_start) ad
on a.date = ad.date_start
left join
(select date_start, sum(sessions) sessions ,
    sum(case when is_ad_order = True then sessions end) Paid_sessions,
   sum(case when is_ad_order != True then sessions
    end) Non_Paid_sessions
 from `noted-computing-279322.halo_1_1.fUAInsights` group by date_start) ua
 on a.date= ua.date_start
 left join
(select date_start,sum(Allvisits)  Allvisits,
                   sum(Productviews) as Productviews,
                   sum( Addstocart) as Addstocart,
                   sum(Checkout) as Checkout,
                   sum(Shipping) as Shipping,
                   sum(PaymentOption) as PaymentOption,
                   sum(PaymentClick) as PaymentClick,
                   sum(Transactions) as Transactions,
                   sum(AddstocartPage) as AddstocartPage,
                   sum(LogginPage) as LogginPage,
                   sum(ShippingPage) as ShippingPage,
                   sum(PaymentPage) as PaymentPage
                   from `noted-computing-279322.halo_1_1.fShoppingStages` group by date_start) ss
on a.date=ss.date_start
left join
(select
order_date date_start,
sum(product_quantity) total_product_quantity,
sum(total_item_price) total_item_price,
sum(case when is_ad_order = True then total_item_price end) total_ad_revenue,
sum(item_discount) total_item_discount,
safe_divide(sum(item_discount),sum(total_item_price)) per_discount,
count(distinct order_id) orders,
count(distinct user_id) users,
sum(case when order_status in ('successful')  then product_quantity end) total_product_quantity_auth,
sum(case when order_status in ('successful')  then total_item_price end) total_item_price_auth,
sum(case when order_status in ('successful')  and is_ad_order = True then total_item_price end) total_ad_revenue_auth,
sum(case when order_status in ('successful')  and (is_ad_order = False or is_ad_order is null) then total_item_price end) total_non_ad_revenue_auth,
sum(case when order_status in ('successful')  then item_discount end) total_item_discount_auth,
safe_divide(sum(case when order_status in ('successful')  then item_discount end),sum(case when order_status in ('successful') then total_item_price end)) per_discount_auth,
count(distinct case when order_status in ('successful')  then order_id end) orders_auth,
count(distinct case when order_status in ('successful')  then user_id end) users_auth,
count(distinct case when order_status in ('successful') and is_ad_order = True then order_id end) Paid_orders,
count(distinct case when order_status in ('successful') and (is_ad_order = False or is_ad_order is null) then order_id end) Non_Paid_orders,
sum(case when order_status in ('successful')  and is_new_customer =1 then total_item_price end) New_Customer_Revenue,
sum(case when order_status in ('successful')  and is_new_customer =0 then total_item_price end) Existing_Customer_Revenue,
count(distinct case when order_status in ('successful')  and is_new_customer = 1 then order_id end) New_Customer_Orders,
count(distinct case when order_status in ('successful') and is_new_customer = 0 then order_id end) Existing_Customer_Orders,
count(distinct case when order_status in ('successful') and is_new_customer = 1 then user_id end) New_Customers,
count(distinct case when order_status in ('successful')  and is_new_customer = 0 then user_id end) Existing_Customers,
sum(case when order_status in ('successful')  then total_item_price_wo_discount end) GrossRevenue,
sum(case when order_status in ('successful')  then Net_Revenue end)  NetRevenue,
sum(case when order_status in ('successful')  then WebPrice end) MSRP,
sum(case when order_status = 'Returned'  then Net_Revenue end) ReturnsRevenue,
sum(case when order_status in ('successful') and is_item_returned=true  then product_quantity end) ReturnedUnits,
sum(case when order_status in ('successful')  and is_ad_order = True then Net_Revenue end) PaidNetRevenue,
sum(case when order_status in ('successful')  and (is_ad_order = False or is_ad_order is null) then Net_Revenue end) NonPaidNetRevenue,
sum(case when order_status ='Canceled' then product_quantity end) Itemscancelled,
count(distinct case when order_status = 'canceled' then order_id end) cancelled_orders,
count(distinct case when checkout_type in ('cashondelivery') and order_status in ('successful') then order_id end) cod,
count(distinct case when checkout_type in ('cardDelivery','payfort_fort_cc','payfort_fort_installments','gurubhyo_premiumcards','checkoutcom_card_payment') and order_status in ('successful') then order_id end) cc,
count(distinct case when checkout_type in ('tabby_installments') and order_status in ('successful') then order_id end) tabby,
count(distinct case when order_status = 'Returned' then order_id end) returned_orders,
sum(case when order_status in ('successful')  then tax_amount end) tax_amount,
sum(case when influencerscode is not null and order_status in ('successful') then total_item_price end)influencersrevenue,
sum(total_item_price) Total_revenue,
sum(distinct case when order_status = 'unsucessful' then total_item_price end) unsucessful_revenue,
count(distinct case when influencerscode is not null and order_status in ('successful') then order_id end)influencersorders,
count(distinct case when order_status = 'unsucessful' then order_id end) unsucessful_orders
--
from `noted-computing-279322.halo_1_1.fOrders`
group by order_date) store
on a.date=store.date_start
left join
`noted-computing-279322.halo_1_1.fCustJourney` us
on a.date=us.date_start
left join
(select order_date,sum(MRP) NonDiscountedSales,sum(Selling_Price) DiscountedSales
from (
select a.*,b.MRP*a.product_quantity MRP,b.Selling_Price*a.product_quantity Selling_Price
from `noted-computing-279322.halo_1_1.fOrders` a left join `noted-computing-279322.halo_1_1.fProductStock` b
on a.product_id=cast(b.productid as string)
)
where 
Is_Successful_Order =True
group by 1) merc
on a.date=merc.order_date
left join
(select date_start,sum(impressions) Impressions,sum(clicks) Clicks,sum(productdetail_views) ProductDetailViews,
       sum(Addstocart) ProductAddstocart
from `noted-computing-279322.halo_1_1.fProductInsights`
group by 1) prod
on a.date=prod.date_start
left join
(select avg(cast(M_ga_avgSessionDuration as float64)) avgSessionDuration,
avg(cast(M_ga_avgPageLoadTime as float64)) avgPageLoadTime,
sum(cast(M_ga_exits as int64)) exits,
sum(cast(M_ga_sessions as int64)) sessions,
StartDate from `noted-computing-279322.halo_1_1.fGABasePages`
group by StartDate)pg
on a.date=pg.StartDate
left join
(select StartDate LP_StartDate,
--,D_ga_deviceCategory LP_deviceCategory,D_ga_browser LP_browser,
sum(cast(M_ga_bounces as int64)) as LP_Lan_Bounce,sum(cast(M_ga_sessions as int64)) LP_Lan_Sessions,
-- sum(cast(M_ga_sessions as int64)) sessions,sum(cast(M_ga_transactions as int64)) tran,
sum(cast(M_ga_transactions as int64)) as LP_Lan_Transactions
 from `noted-computing-279322.halo_1_1.fGABaseLandingPages`
where  lower(D_ga_landingPagePath) like '%/necklaces%' or lower(D_ga_landingPagePath) like '%/earrings%' or lower(D_ga_landingPagePath) like '%/rings%' or lower(D_ga_landingPagePath) like '%/bracelets%' or lower(D_ga_landingPagePath) like '%/anklets%' or lower(D_ga_landingPagePath) like '%/lady-fozaza%' or lower(D_ga_landingPagePath) like '%/shop-all%' or lower(D_ga_landingPagePath) like '%/offer%' or lower(D_ga_landingPagePath) like '%/new-arrival%' or lower(D_ga_landingPagePath) like '%/gif%'
group by 1
--,2,3
--order by 1
--,2,3
) LP_Lan
on a.date=LP_Lan.LP_StartDate
left join
(select StartDate H_StartDate ,
--D_ga_deviceCategory H_deviceCategory,D_ga_browser H_browser,
sum(cast(M_ga_bounces as int64)) H_Lan_Bounces,sum(cast(M_ga_sessions as int64))  H_Lan_Sessions,
--sum(cast(M_ga_sessions as int64)) sessions,sum(cast(M_ga_transactions as int64)) tran,
sum(cast(M_ga_transactions as int64)) H_Lan_transactions  from `noted-computing-279322.halo_1_1.fGABaseLandingPages`
where D_ga_landingPagePath in ('/sa_en/','/sa_ar/','/')
group by 1
--,2,3
--order by 1
--,2,3
) H_Lan
on a.date=H_Lan.H_StartDate
left join
(select StartDate P_StartDate,
--D_ga_deviceCategory P_deviceCategory,D_ga_browser P_browser,
sum(cast(M_ga_bounces as int64)) P_bounces,sum(cast(M_ga_sessions as int64)) P_Sessions,
--sum(cast(M_ga_sessions as int64)) sessions,sum(cast(M_ga_transactions as int64)) tran,
sum(cast(M_ga_transactions as int64)) P_Transactions
from `noted-computing-279322.halo_1_1.fGABaseLandingPages`
where  lower(D_ga_landingPagePath) like '%/product/%'
group by 1
--,2,3
--order by 1
--,2,3
) P_Lan
on a.date=P_Lan.P_StartDate
left join
(select StartDate,sum(cast(M_ga_sessions as int64)) H_Sessions,
sum(cast(M_ga_transactions as int64)) H_Transactions,
((avg(cast(M_ga_avgSessionDuration as float64)))/60 * sum(cast(M_ga_sessions as int64)) ) H_Duration,
null H_AvgsesDur,null H_Pages_Per_Sess,
avg(cast(M_ga_avgPageLoadTime as float64)) H_AvgPgLT,
sum(cast(M_ga_bounces as int64))H_Bounces
from `noted-computing-279322.halo_1_1.fGABaseLandingPages`
where D_ga_landingPagePath in ('/sa_en/','/sa_ar/','/')
group by 1) Home_Pg
on a.date=Home_Pg.StartDate
left join
(select sum(cast(M_ga_pageviews as int64)) as impressions,
sum(cast(M_ga_productListClicks as int64)) as Clicks,sum(cast(M_ga_productAddsToCart as int64)) addstocart,
 sum(cast(M_ga_productDetailViews as int64)) as productDetailViews,
StartDate
from `noted-computing-279322.halo_1_1.fGABaseLandingPages`
where  lower(D_ga_landingPagePath) like '%/necklaces%' or lower(D_ga_landingPagePath) like '%/earrings%' or lower(D_ga_landingPagePath) like '%/rings%' or lower(D_ga_landingPagePath) like '%/bracelets%' or lower(D_ga_landingPagePath) like '%/anklets%' or lower(D_ga_landingPagePath) like '%/lady-fozaza%' or lower(D_ga_landingPagePath) like '%/shop-all%' or lower(D_ga_landingPagePath) like '%/offer%' or lower(D_ga_landingPagePath) like '%/new-arrival%' or lower(D_ga_landingPagePath) like '%/gif%'
group by StartDate) List_pg
on a.date=List_pg.StartDate
) dailyagg1
left join
(select StartDate as date_level,
sum(cast(M_ga_pageviews  as int64)) pageviews
 from `noted-computing-279322.halo_1_1.fGABaseLandingPages`
group by 1) landing1
on dailyagg1.date=landing1.date_level
))dailyagg2
left join
(select distinct date_level,
sum(case when D_ga_deviceCategory = 'mobile' then sessions end)mobile,
sum(case when D_ga_deviceCategory = 'tablet' then sessions end) tablet,
sum(case when D_ga_deviceCategory = 'desktop' then sessions end) desktop
from (
select distinct (parse_date('%Y%m%d',(cast(D_ga_date as string)))) as date_level,
D_ga_deviceCategory,
sum(cast(M_ga_sessions as int64)) Sessions
 from `noted-computing-279322.halo_1_1.fGADeviceBrowser`
group by 1,2)group by 1) devicebrowser
on dailyagg2.date=devicebrowser.date_level
) dailyagg3
left join
(select distinct (parse_date('%Y%m%d',(cast(D_ga_date as string)))) as date_level,
sum(cast(M_ga_newUsers as int64))age_new_users,
sum(case when D_ga_userAgeBracket='18-24' then cast(M_ga_newUsers as INT64) end) newusers_with_age_18_24,
sum(case when D_ga_userAgeBracket='25-34' then cast(M_ga_newUsers as INT64) end) newusers_with_age_25_34,
sum(case when D_ga_userAgeBracket='35-44' then cast(M_ga_newUsers as INT64) end) newusers_with_age_35_44,
sum(case when D_ga_userAgeBracket='45-54' then cast(M_ga_newUsers as INT64)
when D_ga_userAgeBracket='55-64' then cast(M_ga_newUsers as INT64)
when D_ga_userAgeBracket='65+' then cast(M_ga_newUsers as INT64) end) newusers_with_age_above_45,
sum(case when D_ga_userAgeBracket='18-24' then cast(M_ga_sessions as INT64) end) sessions_with_age_18_24,
sum(case when D_ga_userAgeBracket='25-34' then cast(M_ga_sessions as INT64) end) sessions_with_age_25_34,
sum(case when D_ga_userAgeBracket='35-44' then cast(M_ga_sessions as INT64) end) sessions_with_age_35_44,
sum(case when D_ga_userAgeBracket='45-54' then cast(M_ga_sessions as INT64)
when D_ga_userAgeBracket='55-64' then cast(M_ga_sessions as INT64)
when D_ga_userAgeBracket='65+' then cast(M_ga_sessions as INT64) end) sessions_with_age_above_45,
sum(cast(M_ga_sessions as INT64)) age_sessions
from `noted-computing-279322.halo_1_1.fGAAge`
group by 1)age_level
on dailyagg3.date = age_level.date_level)age_fkpi
left join
(select distinct (parse_date('%Y%m%d',(cast(D_ga_date as string)))) as date_gender,
sum(cast(M_ga_newUsers as int64))gender_new_users,
sum(case when D_ga_userGender = 'female' then cast(M_ga_newUsers as INT64) end) newUsers_with_gender_female,
sum(case when D_ga_userGender = 'male' then cast(M_ga_newUsers as INT64) end) newUsers_with_gender_male,
sum(case when D_ga_userGender = 'female' then cast(M_ga_sessions as INT64) end) sessions_with_gender_female,
sum(case when D_ga_userGender = 'male' then cast(M_ga_sessions as INT64) end) sessions_with_gender_male,
sum(cast (M_ga_sessions as INT64)) gender_sessions
from `noted-computing-279322.halo_1_1.fGAGender`
group by 1)gender_level
on age_fkpi.date = gender_level.date_gender)dailyagg4
left join
(select distinct (parse_date('%Y%m%d',(cast(D_ga_date as string)))) as date_level,
sum(case when D_ga_userType = 'New Visitor' then cast(M_ga_sessions as INT64) end) new_visitor_sessions,
sum(case when D_ga_userType = 'Returning Visitor' then cast(M_ga_sessions as INT64) end) returning_visitor_sessions,
from `noted-computing-279322.halo_1_1.fGABaseVisitors`
group by 1)usertype
on dailyagg4.date = usertype.date_level)dailyagg5
left join
(select distinct date as date_level,
sum(case when Channel = "Snapchat_paid" then Sessions end) Snapchat_paid_sessions,
sum(case when Channel = "Snapchat_Unpaid" then Sessions end) Snapchat_Unpaid_sessions,
null as Facebook_Inhouse_Paid_sessions,
sum(case when Channel = "Facebook_Dynamic_Remarketing" then Sessions end) Facebook_Dynamic_Remarketing_sessions,
sum(case when Channel = "Facebook_Unpaid" then Sessions end) Facebook_Unpaid_sessions,
sum(case when Channel = "Instagram_paid" then Sessions end) Instagram_paid_sessions,
sum(case when Channel = "Instagram_Unpaid" then Sessions end) Instagram_Unpaid_sessions,
sum(case when Channel = "TikTok_paid" then Sessions end) TikTok_paid_sessions,
sum(case when Channel = "TikTok_Unpaid" then Sessions end) TikTok_Unpaid_sessions,
sum(case when Channel = "Other_Social" then Sessions end) Other_Social_sessions,
sum(case when Channel = "Paid_Search" then Sessions end) Paid_Search_sessions,
sum(case when Channel = "Internal_Referral" then Sessions end) Internal_Referral_sessions,
sum(case when Channel = "Email" then Sessions end) Email_sessions,
sum(case when Channel = "Organic_Search" then Sessions end) Organic_Search_Sessions,
sum(case when Channel = "Referral" then Sessions end) Referral_Sessions,
sum(case when Channel = "Direct" then Sessions end) Direct_Sessions,
sum(case when Channel = "Others" then Sessions end) Others_Sessions
from
(select distinct (parse_date('%Y%m%d',(cast(D_ga_date as string)))) as date,
case when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)snapchat.*') and REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)paid.*|.*(?i)cpc.*') then 'Snapchat_paid'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)snapchat.*') then 'Snapchat_Unpaid'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)Facebook.*|.*(?i)fb.*') and REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)cpc.*|.*(?i)paid.*') then 'Facebook_Dynamic_Remarketing'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)Facebook.*|.*(?i)fb.*') then 'Facebook_Unpaid'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)Instagram.*|.*(?i)IG.*') and D_ga_campaign like '%Instagram%'then 'Instagram_Unpaid'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)Instagram.*|.*(?i)IG.*') then 'Instagram_paid'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)TikTok.*') and REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)paid.*|.*(?i)cpc.*') then 'TikTok_paid'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)TikTok.*') then 'TikTok_Unpaid'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)lazurde.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)payfort.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)gurubhyo.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)missl.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)analytics.google.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)hotjar.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)roihunter.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)leanport.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)zendesk.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)aramex.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)salesforce.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)skype.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)saras.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)optimize.google.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)adwords.corp.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)ads.google.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)googleads.g.doubleclick.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)fedex.*') then 'Internal_Referral'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)pinterest.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)youtube.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)twitter.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)linkedin.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)t.co*') then 'Other_Social'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)google.*') and REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)cpc.*|.*(?i)paid.*') then 'Paid_Search'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)email.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)mail.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)sendinblue.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)outlook.*') then 'Email'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)yahoo.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)ecosia.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)google.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)bing.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)ask.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)baidu.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)qwant.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)duckduckgo.*') then 'Organic_Search'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)referral.*') then 'Referral'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)direct.*') then 'Direct'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)Facebook.*|.*(?i)fb.*') and REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)paid.*|.*(?i)cpc.*') then 'Facebook_Inhouse_paid'
else 'Others' end Channel,sum(cast(M_ga_sessions as INT64)) sessions
from `noted-computing-279322.halo_1_1.fGACampaigncost`
group by date,channel)
group by 1)sourcemedium
on dailyagg5.date = sourcemedium.date_level)dailyagg6
left join
(select distinct StartDate as date_level,
sum(cast( M_ga_users as int64)) day_level_users,
sum(cast( M_ga_newUsers as int64)) day_level_new_users
from `noted-computing-279322.halo_1_1.fGADaylevelusers` group by 1)day_level_users
on dailyagg6.date = day_level_users.date_level))dailyagg7
left join
(select distinct startdate as date_level,
 sum(sessionduration)sessionduration
from(select *, (cast(M_ga_avgSessionDuration as float64) * cast(M_ga_sessions as int64)) SessionDuration
from `noted-computing-279322.halo_1_1.fGABasePages`)
group by 1)sessionduration_table
on dailyagg7.date = sessionduration_table.date_level))dailyagg8
left join
(select distinct date as date_level,sum(cast(daylevel as INT64))daylevel from
(select *,case when date = date then '1' end daylevel from `noted-computing-279322.halo_1_1.Calendar`)
group by 1)calendar1
on dailyagg8.date = calendar1.date_level))dailyagg9
left join
(select distinct date as date_level,
sum(case when Channel = "Snapchat_paid" then newusers end) Snapchat_paid_newusers,
sum(case when Channel = "Snapchat_Unpaid" then newusers end) Snapchat_Unpaid_newusers,
null as Facebook_Inhouse_Paid_newusers,
sum(case when Channel = "Facebook_Dynamic_Remarketing" then newusers end) Facebook_Dynamic_Remarketing_newusers,
sum(case when Channel = "Facebook_Unpaid" then newusers end) Facebook_Unpaid_newusers,
sum(case when Channel = "Instagram_paid" then newusers end) Instagram_paid_newusers,
sum(case when Channel = "Instagram_Unpaid" then newusers end) Instagram_Unpaid_newusers,
sum(case when Channel = "TikTok_paid" then newusers end) TikTok_paid_newusers,
sum(case when Channel = "TikTok_Unpaid" then newusers end) TikTok_Unpaid_newusers,
sum(case when Channel = "Other_Social" then newusers end) Other_Social_newusers,
sum(case when Channel = "Paid_Search" then newusers end) Paid_Search_newusers,
sum(case when Channel = "Internal_Referral" then newusers end) Internal_Referral_newusers,
sum(case when Channel = "Email" then newusers end) Email_newusers,
sum(case when Channel = "Organic_Search" then newusers end) Organic_Search_newusers,
sum(case when Channel = "Referral" then newusers end) Referral_newusers,
sum(case when Channel = "Direct" then newusers end) Direct_newusers,
sum(case when Channel = "Others" then newusers end) Others_newusers,
sum(newusers) newuserssourcemedium
from
(select distinct (parse_date('%Y%m%d',(cast(D_ga_date as string)))) as date,
case when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)snapchat.*') and REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)paid.*|.*(?i)cpc.*') then 'Snapchat_paid'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)snapchat.*') then 'Snapchat_Unpaid'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)Facebook.*|.*(?i)fb.*') and REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)cpc.*|.*(?i)paid.*') then 'Facebook_Dynamic_Remarketing'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)Facebook.*|.*(?i)fb.*') then 'Facebook_Unpaid'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)Instagram.*|.*(?i)IG.*') and D_ga_sourcemedium like '%Instagram%'then 'Instagram_Unpaid'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)Instagram.*|.*(?i)IG.*') then 'Instagram_paid'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)TikTok.*') and REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)paid.*|.*(?i)cpc.*') then 'TikTok_paid'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)TikTok.*') then 'TikTok_Unpaid'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)lazurde.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)payfort.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)gurubhyo.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)missl.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)analytics.google.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)hotjar.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)roihunter.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)leanport.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)zendesk.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)aramex.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)salesforce.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)skype.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)saras.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)optimize.google.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)adwords.corp.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)ads.google.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)googleads.g.doubleclick.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)fedex.*') then 'Internal_Referral'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)pinterest.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)youtube.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)twitter.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)linkedin.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)t.co*') then 'Other_Social'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)google.*') and REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)cpc.*|.*(?i)paid.*') then 'Paid_Search'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)email.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)mail.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)sendinblue.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)outlook.*') then 'Email'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)yahoo.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)ecosia.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)google.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)bing.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)ask.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)baidu.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)qwant.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)duckduckgo.*') then 'Organic_Search'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)referral.*') then 'Referral'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)direct.*') then 'Direct'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)Facebook.*|.*(?i)fb.*') and REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)paid.*|.*(?i)cpc.*') then 'Facebook_Inhouse_paid'
else 'Others' end Channel,sum(cast(M_ga_newusers as INT64)) newusers
from `noted-computing-279322.halo_1_1.fGASourcemediumnewusers`
group by date,channel)
group by 1)sourcemediumnewusers
on dailyagg9.date = sourcemediumnewusers.date_level))dailyagg10
left join
(select distinct date as date_level,
sum(case when code = "missl10" then Spend_per_day end) MissL10_spend_day,
sum(case when code = "husin10" then Spend_per_day end) Husin10_spend_day,
sum(case when code = "feda10" then Spend_per_day end) Feda10_spend_day,
sum(case when code = "rand10" then Spend_per_day end) Rand10_spend_day,
sum(case when code = "satha10" then Spend_per_day end) Satha10_spend_day,
sum(case when code = "wish10" then Spend_per_day end) Wish10_spend_day,
sum(case when code = "abrar21" then Spend_per_day end) abrar21_spend_day,
sum(case when code = "feda21" then Spend_per_day end) feda21_spend_day,
sum(case when code = "roh_n2" then Spend_per_day end) roh_n2_spend_day,
sum(case when code = "omseba10" then Spend_per_day end) omseba10_spend_day,
sum(case when code = "marwa10" then Spend_per_day end) marwa10_spend_day,
sum(case when code = "maha_140" then Spend_per_day end) maha_140_spend_day,
sum(case when code = "shahad10" then Spend_per_day end) shahad10_spend_day,
sum(case when code = "yasmine10" then Spend_per_day end) Yasmine10_spend_day,
sum(case when code = "fatma10" then Spend_per_day end) Fatma10_spend_day,
sum(case when code = "missl2u" then Spend_per_day end) missl2u_spend_day,
sum(case when code = "missl10" then orders end) MissL10_orders,
sum(case when code = "husin10" then orders end) Husin10_orders,
sum(case when code = "feda10" then orders end) Feda10_orders,
sum(case when code = "rand10" then orders end) Rand10_orders,
sum(case when code = "satha10" then orders end) Satha10_orders,
sum(case when code = "wish10" then orders end) Wish10_orders,
sum(case when code = "abrar21" then orders end) abrar21_orders,
sum(case when code = "feda21" then orders end) feda21_orders,
sum(case when code = "roh_n2" then orders end) roh_n2_orders,
sum(case when code = "omseba10" then orders end) omseba10_orders,
sum(case when code = "marwa10" then orders end) marwa10_orders,
sum(case when code = "maha_140" then orders end) maha_140_orders,
sum(case when code = "shahad10" then orders end) shahad10_orders,
sum(case when code = "yasmine10" then orders end) Yasmine10_orders,
sum(case when code = "fatma10" then orders end) Fatma10_orders,
sum(case when code = "missl2u" then orders end) missl2u_orders,
sum(case when code = "missl10" then revenue end) MissL10_revenue,
sum(case when code = "husin10" then revenue end) Husin10_revenue,
sum(case when code = "feda10" then revenue end) Feda10_revenue,
sum(case when code = "rand10" then revenue end) Rand10_revenue,
sum(case when code = "satha10" then revenue end) Satha10_revenue,
sum(case when code = "wish10" then revenue end) Wish10_revenue,
sum(case when code = "abrar21" then revenue end) abrar21_revenue,
sum(case when code = "feda21" then revenue end) feda21_revenue,
sum(case when code = "roh_n2" then revenue end) roh_n2_revenue,
sum(case when code = "omseba10" then revenue end) omseba10_revenue,
sum(case when code = "marwa10" then revenue end) marwa10_revenue,
sum(case when code = "maha_140" then revenue end) maha_140_revenue,
sum(case when code = "shahad10" then revenue end) shahad10_revenue,
sum(case when code = "yasmine10" then revenue end) Yasmine10_revenue,
sum(case when code = "fatma10" then revenue end) Fatma10_revenue,
sum(case when code = "missl2u" then revenue end) missl2u_revenue
from
(select * except(order_date,couponcode) from
(select * except(monthcode,yearcode,month,year) from
(select distinct date,month,year from `noted-computing-279322.halo_1_1.Calendar`)a
left join
(select distinct lower(code)code, Spend_per_day,month as monthcode,year as yearcode
from `noted-computing-279322.halo_1_1.magento_influencerscode`
where currency = 'SAR')b
ON a.month = b.monthcode and a.year = b.yearcode)c
left join
(select distinct order_date,count(distinct increment_id) orders,sum(total_item_price) revenue,couponcode from
(select distinct order_date,increment_id,sum(total_item_price)total_item_price,lower(coupon_code)couponcode
from `noted-computing-279322.halo_1_1.fOrders`
where order_status = 'successful'
group by order_date,increment_id,couponcode)
where couponcode is not null
group by order_date,couponcode)d
ON c.date = d.order_date and c.code = d.couponcode)
group by date_level) customer_acquisition
on dailyagg10.date = customer_acquisition.date_level))dailyagg11
left join
(select *except(date_level) from
(select *except(start_date) from
(select *except(date_level) from
(select *except(date_level) from
(select distinct date as date_level1 from `noted-computing-279322.halo_1_1.Calendar`
where date >= '2020-01-01')a
left join
(select distinct date as date_level,
null as Facebook_cust_traffic,
-- sum(case when customer_acquisition = "Facebook_Inhouse_paid" then Sessions end) Facebook_cust_traffic,
sum(case when customer_acquisition = "Instagram_paid" then Sessions end) Instagram_cust_traffic,
sum(case when customer_acquisition = "Snapchat_paid" then Sessions end) Snapchat_cust_traffic,
sum(case when customer_acquisition = "Paid_Search" then Sessions end) Adwords_cust_traffic,
sum(case when customer_acquisition = "Facebook_Dynamic_Remarketing" then Sessions end) Roi_hunter_cust_traffic,
sum(case when customer_acquisition = "Youtube_paid" then Sessions end) Youtube_cust_traffic,
sum(case when customer_acquisition = "Tiktok_paid" then Sessions end) Tiktok_cust_traffic,
null as Facebook_cust_orders,
-- sum(case when customer_acquisition = "Facebook_Inhouse_paid" then orders end) Facebook_cust_orders,
sum(case when customer_acquisition = "Instagram_paid" then orders end) Instagram_cust_orders,
sum(case when customer_acquisition = "Snapchat_paid" then orders end) Snapchat_cust_orders,
sum(case when customer_acquisition = "Paid_Search" then orders end) Adwords_cust_orders,
sum(case when customer_acquisition = "Youtube_paid" then orders end) Youtube_cust_orders,
sum(case when customer_acquisition = "Tiktok_paid" then orders end) Tiktok_cust_orders,
sum(case when customer_acquisition = "Facebook_Dynamic_Remarketing" then orders end) Roi_hunter_cust_orders,
null as Facebook_cust_revenue,
-- sum(case when customer_acquisition = "Facebook_Inhouse_paid" then revenue end) Facebook_cust_revenue,
sum(case when customer_acquisition = "Instagram_paid" then revenue end) Instagram_cust_revenue,
sum(case when customer_acquisition = "Snapchat_paid" then revenue end) Snapchat_cust_revenue,
sum(case when customer_acquisition = "Paid_Search" then revenue end) Adwords_cust_revenue,
sum(case when customer_acquisition = "Youtube_paid" then revenue end) Youtube_cust_revenue,
sum(case when customer_acquisition = "Tiktok_paid" then revenue end) Tiktok_cust_revenue,
sum(case when customer_acquisition = "Facebook_Dynamic_Remarketing" then revenue end) Roi_hunter_cust_revenue
from
(select *except(date_level,customer_acqui) from
(select * from
(select distinct parse_date('%Y%m%d',(cast(D_ga_date as string))) as date,
case
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)snapchat.*') and REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)paid.*|.*(?i)cpc.*') then 'Snapchat_paid'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)Facebook.*|.*(?i)fb.*') and REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)cpc.*|.*(?i)paid.*') then 'Facebook_Dynamic_Remarketing'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)Instagram.*|.*(?i)IG.*') and D_ga_campaign not like '%Instagram%'then 'Instagram_paid'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)google.*') and REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)cpc.*|.*(?i)paid.*') then 'Paid_Search'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)Facebook.*|.*(?i)fb.*') and REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)paid.*|.*(?i)cpc.*') then 'Facebook_Inhouse_paid'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)Youtube.*|.*(?i)youtube.*') and REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)paid.*|.*(?i)cpc.*') then 'Youtube_paid'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)tiktok.*|.*(?i)Tiktok.*') and REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)paid.*|.*(?i)cpc.*') then 'Tiktok_paid'
end customer_acquisition,sum(cast(M_ga_sessions as INT64)) sessions
from `noted-computing-279322.halo_1_1.fGACampaigncost`
group by date,customer_acquisition)
where customer_acquisition is not null)gacost
left join
(select distinct date_level,
case when customer_acqui  is null then 'not in GA'
else customer_acqui end customer_acqui,
orders,revenue from
(
(select distinct date_level,customer_acqui,count(distinct increment_id)orders,sum(total_item_price)revenue from
(select *except(d_ga_transactionid) from
(select * from
(select
case when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)snapchat.*') and REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)paid.*|.*(?i)cpc.*') then 'Snapchat_paid'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)snapchat.*') then 'Snapchat_Unpaid'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)Facebook.*|.*(?i)fb.*') and REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)cpc.*|.*(?i)paid.*') then 'Facebook_Dynamic_Remarketing'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)Facebook.*|.*(?i)fb.*') then 'Facebook_Unpaid'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)Instagram.*|.*(?i)IG.*') and D_ga_campaign like '%Instagram%'then 'Instagram_Unpaid'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)Instagram.*|.*(?i)IG.*') then 'Instagram_paid'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)Youtube.*|.*(?i)youtube.*') and REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)paid.*|.*(?i)cpc.*') then 'Youtube_paid'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)tiktok.*|.*(?i)Tiktok.*') and REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)paid.*|.*(?i)cpc.*') then 'Tiktok_paid'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)TikTok.*') then 'TikTok_Unpaid'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)lazurde.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)payfort.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)gurubhyo.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)missl.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)analytics.google.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)hotjar.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)roihunter.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)leanport.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)zendesk.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)aramex.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)salesforce.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)skype.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)saras.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)optimize.google.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)adwords.corp.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)ads.google.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)googleads.g.doubleclick.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)fedex.*') then 'Internal_Referral'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)pinterest.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)twitter.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)linkedin.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)t.co*') then 'Other_Social'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)google.*') and REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)cpc.*|.*(?i)paid.*') then 'Paid_Search'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)email.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)mail.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)sendinblue.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)outlook.*') then 'Email'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)yahoo.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)ecosia.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)google.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)bing.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)ask.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)baidu.*') or
REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)qwant.*') or REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)duckduckgo.*') then 'Organic_Search'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)referral.*') then 'Referral'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)direct.*') then 'Direct'
when REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)Facebook.*|.*(?i)fb.*') and REGEXP_CONTAINS(D_ga_sourcemedium,'.*(?i)paid.*|.*(?i)cpc.*') then 'Facebook_Inhouse_paid'
else 'Others' end customer_acqui,d_ga_transactionid
from `noted-computing-279322.halo_1_1.fGABaseTransactions`)
)a
right join
(select distinct increment_id,sum(total_item_price)total_item_price,order_date as date_level from `noted-computing-279322.halo_1_1.fOrders`
where order_status = 'successful' group by 1,3)b
ON a.d_ga_transactionid = b.increment_id
where increment_id is not null)
group by date_level,customer_acqui)
))gatransaction
ON gacost.date = gatransaction.date_level and gacost.customer_acquisition = gatransaction.customer_acqui)
group by date_level)b
ON a.date_level1 = b.date_level)c
left join
(select distinct date_level,
null as Facebook_cust_spend,
sum(case when cust_acquisition = "Instagram_spend" then amount_spend end) Instagram_cust_spend,
sum(case when cust_acquisition = "roi_hunter_spend" then amount_spend end) roi_hunter_cust_spend
from
(select *,
case
when lower(platform) = 'instagram' and retargeting = 'False' then 'Instagram_spend'
-- when lower(platform) = 'messenger' and retargeting = 'False' then 'Facebook_spend'
-- when lower(platform) = 'audience_network' and retargeting = 'False' then 'Facebook_spend'
when lower(platform) in ('facebook','messenger','audience_network') then 'roi_hunter_spend'
-- when lower(platform) = 'facebook' and retargeting = 'False' then 'Facebook_spend'
end cust_acquisition from
(select distinct cast(date_start as date)date_level,campaign_id, campaign_name,platform,
sum(cost)amount_spend,Retargeting
from `noted-computing-279322.halo_1_1.magento_facebookflatfile`
where country = 'KSA'
group by date_level,campaign_id,campaign_name,platform,retargeting))
group by date_level)d
ON c.date_level1 = d.date_level)g
left join
(select distinct start_date,
sum(case when lower(snapchat_flag) = 'snapchat_paid' then total_spend end) snapchat_cust_spend from
(select start_date,sum(Amount_Spent) as total_spend,snapchat_flag
from
(select Date(cast(Start_Time as date)) as start_date,Campaign_Id,campaign_name,Amount_Spent,"Snapchat_paid" as snapchat_flag
from `noted-computing-279322.halo_1_1.magento_snapchatflatfile`
-- where Start_time <> ""
)
group by start_date,snapchat_flag)
group by start_date)h
ON g.date_level1 = h.start_date)i
left join
(select distinct date_level,
sum(case when customer_acquisition = 'Adwords' then adcost end) google_cust_adspend,
 sum(case when customer_acquisition = 'Adwords' then google_revenue end) google_cust_revenue from
(select distinct parse_date('%Y%m%d',(cast(D_ga_date as string))) as date_level,
case when D_ga_sourcemedium = 'google / cpc' or D_ga_sourcemedium = 'google / paid' then 'Adwords' end customer_acquisition,
sum(cast(M_ga_adCost as float64)) adcost,
sum(cast(M_ga_transactionRevenue as float64)) google_revenue,
from `noted-computing-279322.halo_1_1.fGACampaigncost`
-- where lower( D_ga_campaign ) like '%sa%' and lower( D_ga_campaign ) not like '%eg%'
group by date_level,customer_acquisition)
group by date_level)j
ON i.date_level1 = j.date_level) customer_acquisition1
on dailyagg11.date = customer_acquisition1.date_level1))dailyagg12
left join
(select date as date_level1,target
from `noted-computing-279322.halo_1_1.magento_target`)target
on dailyagg12.date = target.date_level1))dailyagg13
left join
-- OPERATIONS KPIS (CONFIRMED, SHIPPED & DELIVERED ORDERS)
(select distinct created_date date_level1,
count(distinct case when status = 'confirmed' then increment_id end)orders_confirmed,
count(distinct case when status = 'shipped' then increment_id end)orders_shipped,
count(distinct case when status like 'delivered' then increment_id end)orders_delivered
from `noted-computing-279322.halo_1_1.magento_transaction_operations`
group by 1)operationsKPI
on dailyagg13.date = operationsKPI.date_level1))dailyagg14
left join
-- OPERATIONS KPIS (TIME)
(select created_date date_level1,round(sum(time_to_confirm),2)total_time_to_confirm,round(sum(time_to_ship),2)total_time_to_ship,
round(sum(time_to_deliver),2)total_time_to_deliver,round(sum(time_to_fulfill),2)total_time_to_fulfill,
from
(
select distinct increment_id,created_date,confirmed_date,shipped_date,delivered_date,date_diff(confirmed_date,created_date,day)time_to_confirm,
date_diff(shipped_date,confirmed_date,day)time_to_ship,date_diff(delivered_date,shipped_date,day)time_to_deliver,
date_diff(delivered_date,confirmed_date,day)time_to_fulfill
from
(
select increment_id,created_date,
min(case when status = 'confirmed' then updated_date end) confirmed_date,
min(case when status = 'shipped' then updated_date end) shipped_date,
min(case when status = 'delivered' then updated_date end) delivered_date
from `noted-computing-279322.halo_1_1.magento_transaction_operations`
group by 1,2))
group by 1)operationskpi1
on dailyagg14.date = operationskpi1.date_level1))dailyagg15
left join
(select * from `noted-computing-279322.halo_1_1.magento_sales_target`)marketingtarget
on dailyagg15.date = marketingtarget.date_level1))dailyagg16
left join
(select date as date_level1,
sum(case when channel = 'facebook & instagram' then target_per_day end) facebook_target_day,
sum(case when channel = 'snapchat' then target_per_day end) snapchat_target_day,
sum(case when channel = 'adwords' then target_per_day end) adwords_target_day,
sum(case when channel = 'tiktok' then target_per_day end) tiktok_target_day,
sum(case when channel = 'youtube' then target_per_day end) youtube_target_day,
from
(select * except(monthcode,yearcode,month,year) from
(select distinct date,month,year from `noted-computing-279322.halo_1_1.Calendar`)a
left join
(select distinct lower(channel)channel, Target_per_day,month as monthcode,year as yearcode
from `noted-computing-279322.marketing_goals_trial.Marketing_trial`
where currency = 'SAR')b
ON a.month = b.monthcode and a.year = b.yearcode)c
group by 1)marketingchanneltarget
on dailyagg16.date = marketingchanneltarget.date_level1))dailyagg17
left join
-- OPERATIONS (ON TIME & DELAYED SHIPMENTS)
(select created_date date_level1,
count(distinct case when on_time_del = "yes" then shipment_id end) on_time_shipments,
count(distinct case when on_time_del = "no" then shipment_id end) delayed_shipments,
from
(select *,if(actual_del_date <= expected_del_date,"yes","no")on_time_del
from
(select distinct created_date,shipment_id,expected_del_date,actual_del_date
from
(select distinct awb shipment_id,status_category status,total_delivery_attempts,expected_delivery_date expected_del_date,
transit_days,Pickup_Date__Creation_Date_ created_date,date(Delivery_Status_Action_Date)actual_del_date,
from
(select *
from `noted-computing-279322.halo_1_1.aramex`
where lower(origin_country) like '%saudi%'))
where shipment_id is not null))
where created_date is not null
group by 1)operationkpi2
on dailyagg17.date = operationkpi2.date_level1))dailyagg18
left join
-- OPERATIONS (NOT DELIVERED/REFUSED)
(select created_date date_level1,
count(distinct case when refused = 'yes' then shipment_id end) refused_shipments,
from
(select *,
if(status = 'Refused','yes','no') refused
from
(select distinct awb shipment_id,status_category status,Pickup_Date__Creation_Date_ created_date
from `noted-computing-279322.halo_1_1.aramex`
where awb is not null
and lower(origin_country) like '%saudi%'))
group by 1)operationkpi3
on dailyagg18.date = operationkpi3.date_level1))dailyagg19
left join
-- OPERATIONS (AVERAGE DELIVERY ATTEMPTS OF SHIPMENTS)
(select created_date date_level1,count(distinct shipment_id) shipments,sum(total_delivery_attempts)total_attempts
from
(select distinct awb shipment_id,total_delivery_attempts,Pickup_Date__Creation_Date_ created_date
from `noted-computing-279322.halo_1_1.aramex`
where awb is not null
and lower(origin_country) like '%saudi%'
and total_delivery_attempts > 0)
group by 1)operationkpi4
on dailyagg19.date = operationkpi4.date_level1))dailyagg20
left join
-- CRM REPORT (TICKETS BY CHANNEL)
(select distinct created_date date_level1,
count(distinct id) total_tickets_created,
count(distinct case when channel = 'facebook' then id end) facebook_tickets,
count(distinct case when channel = 'chat' then id end) chat_tickets,
count(distinct case when channel = 'email' then id end) email_tickets,
count(distinct case when channel = 'twitter' then id end) twitter_tickets,
count(distinct case when channel = 'web' then id end) web_tickets,
count(distinct case when channel not in ('web','twitter','email','chat','facebook') then id end) other_cases
from
(
select distinct a.id,b.channel,date(datetime(cast(a.created_at as timestamp),'Asia/Riyadh'))created_date
from `noted-computing-279322.MissL1.zendesk_Tickets` a, unnest(via) b)
group by 1)crm
on dailyagg20.date = crm.date_level1))dailyagg21
left join
-- CRM REPORT (AVERAGE COMPLAINT RESOLVING TIME IN DAYS)
(select created_date date_level1,sum(days_to_close) as total_days_to_close
from
(
select *,date_diff(updated_date,created_date,day) days_to_close
from
(
select distinct id,date(datetime(cast(created_at as timestamp),'Asia/Riyadh'))created_date,
date(datetime(cast(updated_at as timestamp),'Asia/Riyadh'))updated_date
from `noted-computing-279322.MissL1.zendesk_Tickets`
where status = 'closed'))
group by 1)crm1
on dailyagg21.date = crm1.date_level1))dailyagg22
left join
(-- CRM REPORT (TICKETS BY STATUS)
select distinct created_date date_level1,
count(distinct case when status = 'new' then id end) new_tickets,
count(distinct case when status = 'open' then id end) open_tickets,
count(distinct case when status = 'closed' then id end) closed_tickets,
count(distinct case when status = 'pending' then id end) pending_tickets,
count(distinct case when status = 'solved' then id end) solved_tickets,
count(distinct case when status = 'deleted' then id end) deleted_tickets,
from
(
select distinct id,date(datetime(cast(created_at as timestamp),'Asia/Riyadh'))created_date,status
from `noted-computing-279322.MissL1.zendesk_Tickets`)
group by 1)crm2
on dailyagg22.date = crm2.date_level1))dailyagg23
left join
-- CRM REPORT (TICKETS BY COMPLAINT CATEGORY)
(select distinct created_date date_level1,
count(distinct case when lower(tags) like '%return%' then id end) return,
count(distinct case when lower(tags) like '%exchange%' then id end) exchange,
count(distinct case when lower(tags) like '%delay_in_order%' then id end) order_delay,
count(distinct case when lower(tags) like '%wrong_order%' then id end) wrong_order,
count(distinct case when lower(tags) like '%manufacturing%' then id end) manufacturing,
count(distinct case when lower(tags) like '%web%' then id end) website_issue,
count(distinct case when not (lower(tags) like '%return%'
or lower(tags) like '%exchange%' or lower(tags) like '%delay_in_order%' or lower(tags) like '%wrong_order%'
or lower(tags) like '%manufacturing%' or lower(tags) like '%web%') then id end) general_query
from
(
select distinct id,tags,date(datetime(cast(created_at as timestamp),'Asia/Riyadh'))created_date
from `noted-computing-279322.MissL1.zendesk_Tickets`)
group by 1)crm3
on dailyagg23.date = crm3.date_level1))dailyagg24
left join
-- operationsorderscreated
(select distinct created_date date_level1,orders_created
from
(select created_date,count(distinct increment_id)orders_created
from
(select distinct increment_id,date(datetime(created_at,'Asia/Riyadh'))created_date,status
from `noted-computing-279322.halo_1_1.magento_transactionnew`
where store_id in (4,21)
and status <> 'canceled')
group by 1))orderscreated
on dailyagg24.date = orderscreated.date_level1))dailyagg25
left join
(select day as date_level1,round(sum(cost_per_day),2) spend_per_day
from `noted-computing-279322.halo_1_1.youtube`
group by 1)youtube
on dailyagg25.date=youtube.date_level1))dailyagg26
left join
(select date as date_level1,
sum(case when age_type = '>6' then product_count end) sku_more_than_six,
sum(case when age_type = '>3' then product_count end) sku_more_than_three,
sum(case when age_type = '<3' then product_count end) sku_less_than_three
from
(select Date,Country,age_type,count(age_type) as product_count
from
(select distinct prod.sku,Calendar.Date as Date,date_diff(calendar.Date,extract(date from prod.created_at),day) as age,
case when lower(sku) like  '%ksa%' then 'KSA'
     when lower(sku) like '%eg%' then 'Egypt'
     else 'Not known' end country,
     case when date_diff(calendar.Date,extract(date from prod.created_at),day) >180 then '>6'
     when date_diff(calendar.Date,extract(date from prod.created_at),day)> 90 then '>3'
     when date_diff(calendar.Date,extract(date from prod.created_at),day) <90 then '<3'
     end as age_type
from `noted-computing-279322.halo_1_1.Calendar` calendar,`noted-computing-279322.MissL1.magento2_missl_live_catalog_product_entity` prod
where Calendar.Date>extract(date from prod.created_at)
order by 1,2)
group by 3,1,2)
where lower(country) = 'ksa'
group by 1)product_age_count
on dailyagg26.date=product_age_count.date_level1))dailyagg27
left join
(select distinct date_level1,num_of_skus,skus_80_20
from
(
select *,row_number() over (partition by date_level1) rownum
from
(select *,sum(perc_contr) over (partition by date_level1 order by perc_contr desc) perc_contr_sum,row_number() over (partition by date_level1) skus_80_20
from
(
select *except(order_date),100*safe_divide(sku_revenue,revenue) perc_contr
from
(
select *
from
(select order_date date_level1,product_sku,sum(total_item_price) sku_revenue
from `noted-computing-279322.halo_1_1.fOrders`
where order_status = 'successful'
group by 1,2)a
left join
(select order_date,count(distinct product_sku) num_of_skus,sum(total_item_price) revenue
from `noted-computing-279322.halo_1_1.fOrders`
where order_status = 'successful'
group by 1)b
on a.date_level1 = b.order_date)
order by date_level1,sku_revenue desc))
where perc_contr_sum >= 80)
where rownum = 1)sales80
on dailyagg27.date=sales80.date_level1))dailyagg28
left join
(select * except(country) from
(select extract(date from created_at) date_level1,count(distinct sku) as no_launched,
case when lower(sku) like  '%ksa%' then 'KSA'
     when lower(sku) like '%eg%' then 'Egypt'
     else 'Not known' end country
     from `noted-computing-279322.MissL1.magento2_missl_live_catalog_product_entity`
group by 1,3 )
where country='KSA')sku_launch
on dailyagg28.date=sku_launch.date_level1))dailyagg29
left join
(SELECT distinct(cast (date as date)) as date_level1, round(sum(spend_ksa),2) as tiktok_cust_spend
from `noted-computing-279322.tiktok_ksa.tiktok_ksa`
group by 1) tiktok_ksa
on dailyagg29.date=tiktok_ksa.date_level1)
