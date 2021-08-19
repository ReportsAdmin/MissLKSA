
select ta.*,
concat(format_date('%d %b %y',parse_date('%Y-%m-%d',Startdate)),' - ',format_date('%d %b %y',parse_date('%Y-%m-%d',Enddate))) Value_DateRange,
concat(format_date('%d %b %y',parse_date('%Y-%m-%d',PrevX_Startdate)),' - ',format_date('%d %b %y',parse_date('%Y-%m-%d',PrevX_Enddate))) PrevValue_DateRange,
concat(format_date('%d %b %y',parse_date('%Y-%m-%d',PrevYearX_Startdate)),' - ',format_date('%d %b %y',parse_date('%Y-%m-%d',PrevYearX_Enddate))) PrevYearValue_DateRange,
case when lower(KPI) like '%bounce%' and lower(KPI) not like '%non%bounce' then safe_divide(-(Value-prevvalue),prevvalue) else safe_divide((Value-prevvalue),prevvalue) end Change_PrevX,
case when lower(KPI) like '%bounce%' and lower(KPI) not like '%non%bounce' then safe_divide(-(Value-prevyearvalue),prevyearvalue) else safe_divide((Value-prevyearvalue),prevyearvalue) end Change_PrevYearX,
tb.KPI_ID,
tb.KPI_FORMAT
from
(
select
Halo_country,
KPI,
period_flag as XTD,
period_id date,
ifnull(Value,0) Value,

ifnull(lag(Value,1) over (partition by Halo_country,KPI,period_flag order by period_id),0) prevvalue,

ifnull(case when extract(year from parse_date('%Y-%m-%d',Startdate)) - 1 = extract(year from lag(parse_date('%Y-%m-%d',Startdate),1) over (partition by Halo_country,KPI,period_flag
order by  substr(period_id,5), cast(substr(period_id,1,4) as int64)
)) then lag(Value,1) over (partition by Halo_country,KPI,period_flag
order by  substr(period_id,5), cast(substr(period_id,1,4) as int64)
) end,0) prevyearvalue,

Startdate,
Enddate,

lag(Startdate,1) over (partition by Halo_country,KPI,period_flag order by period_id) PrevX_Startdate	,

lag(Enddate,1) over (partition by Halo_country,KPI,period_flag order by period_id) PrevX_Enddate,

case when extract(year from parse_date('%Y-%m-%d',Startdate)) - 1 = extract(year from lag(parse_date('%Y-%m-%d',Startdate),1) over (partition by Halo_country,KPI,period_flag
order by  substr(period_id,5), cast(substr(period_id,1,4) as int64)
)) then lag(Startdate,1) over (partition by Halo_country,KPI,period_flag
order by  substr(period_id,5), cast(substr(period_id,1,4) as int64)
) end PrevYearX_Startdate,

case when extract(year from parse_date('%Y-%m-%d',Enddate)) - 1 = extract(year from lag(parse_date('%Y-%m-%d',Enddate),1) over (partition by Halo_country,KPI,period_flag
order by  substr(period_id,5), cast(substr(period_id,1,4) as int64)
)) then  lag(Enddate,1) over (partition by Halo_country,KPI,period_flag
order by  substr(period_id,5), cast(substr(period_id,1,4) as int64)
) end PrevYearX_Enddate



-- prvValue
from
(
select
Halo_country,
KPI,
coalesce(date,dateweek,datemonth,datequarter,dateyear) period_id,
CASE WHEN datequarter is null THEN 'Year'
WHEN datemonth is null THEN 'Quarter'
WHEN dateweek is null THEN 'Month'
WHEN date is null THEN 'Week'
ELSE 'Day' END period_flag,
case when CASE WHEN datequarter is null THEN 'Y'
WHEN datemonth is null THEN 'Q'
WHEN dateweek is null THEN 'M'
WHEN date is null THEN 'W'
ELSE 'D' END = 'W' then
sum(Value) over(partition by Halo_country, KPI, CASE WHEN datequarter is null THEN 'Y'
WHEN datemonth is null THEN 'Q'
WHEN dateweek is null THEN 'M'
WHEN date is null THEN 'W'
ELSE 'D' END, coalesce(date,dateweek,datemonth,datequarter,dateyear))
else Value end Value,
case when CASE WHEN datequarter is null THEN 'Y'
WHEN datemonth is null THEN 'Q'
WHEN dateweek is null THEN 'M'
WHEN date is null THEN 'W'
ELSE 'D' END = 'W' then
min(Startdate) over(partition by Halo_country, KPI, CASE WHEN datequarter is null THEN 'Y'
WHEN datemonth is null THEN 'Q'
WHEN dateweek is null THEN 'M'
WHEN date is null THEN 'W'
ELSE 'D' END, coalesce(date,dateweek,datemonth,datequarter,dateyear))
else Startdate end Startdate,
case when CASE WHEN datequarter is null THEN 'Y'
WHEN datemonth is null THEN 'Q'
WHEN dateweek is null THEN 'M'
WHEN date is null THEN 'W'
ELSE 'D' END = 'W' then
max(Enddate) over(partition by Halo_country, KPI, CASE WHEN datequarter is null THEN 'Y'
WHEN datemonth is null THEN 'Q'
WHEN dateweek is null THEN 'M'
WHEN date is null THEN 'W'
ELSE 'D' END, coalesce(date,dateweek,datemonth,datequarter,dateyear))
else Enddate end Enddate,
case when CASE WHEN datequarter is null THEN 'Y'
WHEN datemonth is null THEN 'Q'
WHEN dateweek is null THEN 'M'
WHEN date is null THEN 'W'
ELSE 'D' END = 'W' then row_number() over(partition by Halo_country, KPI, CASE WHEN datequarter is null THEN 'Y'
WHEN datemonth is null THEN 'Q'
WHEN dateweek is null THEN 'M'
WHEN date is null THEN 'W'
ELSE 'D' END, coalesce(date,dateweek,datemonth,datequarter,dateyear) order by coalesce(date,dateweek,datemonth,datequarter,dateyear) desc)
else 1 end week_rno
from
(
select
Halo_country,
KPI,
dateyear,
datequarter,
datemonth,
dateweek,
date,
(case when KPI = 'sku_less_than_three' or KPI = 'sku_more_than_six'
 or KPI = 'sku_more_than_three' then max(Value) else sum(Value) end) Value,
min(date) Startdate,
max(date) Enddate
-- sum(case when date <
from
(
select
Halo_country,
FORMAT_DATE('%Y-%m-%d', date) date,
FORMAT_DATE('%Y-%W', date) dateweek,
FORMAT_DATE('%Y-%m', date) datemonth,
FORMAT_DATE('%Y-%Q', date) datequarter,
FORMAT_DATE('%Y', date) dateyear,
Metrics_data.Metric KPI,
Metrics_data.Data Value,
max(date) over () current_date
from (
select
Halo_Country,
date,
Metrics_data
from (
select Halo_Country,date,
       [
       
                STRUCT('total_item_price_auth' as Metric, total_item_price_auth as Data)
            
              ,
            
        
                STRUCT('orders_auth' as Metric, orders_auth as Data)
            
              ,
            
        
                STRUCT('sessions' as Metric, sessions as Data)
            
              ,
            
        
                STRUCT('total_ad_revenue_auth' as Metric, total_ad_revenue_auth as Data)
            
              ,
            
        
                STRUCT('total_non_ad_revenue_auth' as Metric, total_non_ad_revenue_auth as Data)
            
              ,
            
        
                STRUCT('spend' as Metric, spend as Data)
            
              ,
            
        
                STRUCT('New_customers' as Metric, New_customers as Data)
            
              ,
            
        
                STRUCT('New_customer_Revenue' as Metric, New_customer_Revenue as Data)
            
              ,
            
        
                STRUCT('Existing_customer_Revenue' as Metric, Existing_customer_Revenue as Data)
            
              ,
            
        
                STRUCT('Bounce' as Metric, Bounce as Data)
            
              ,
            
        
                STRUCT('Productviews' as Metric, Productviews as Data)
            
              ,
            
        
                STRUCT('Addstocart' as Metric, Addstocart as Data)
            
              ,
            
        
                STRUCT('Checkout' as Metric, Checkout as Data)
            
              ,
            
        
                STRUCT('Shipping' as Metric, Shipping as Data)
            
              ,
            
        
                STRUCT('PaymentOption' as Metric, PaymentOption as Data)
            
              ,
            
        
                STRUCT('PaymentClick' as Metric, PaymentClick as Data)
            
              ,
            
        
                STRUCT('Transactions' as Metric, Transactions as Data)
            
              ,
            
        
                STRUCT('visitors' as Metric, visitors as Data)
            
              ,
            
        
                STRUCT('registered_users' as Metric, registered_users as Data)
            
              ,
            
        
                STRUCT('New_Customer_Orders' as Metric, New_Customer_Orders as Data)
            
              ,
            
        
                STRUCT('Existing_Customer_Orders' as Metric, Existing_Customer_Orders as Data)
            
              ,
            
        
                STRUCT('Paid_orders' as Metric, Paid_orders as Data)
            
              ,
            
        
                STRUCT('Paid_sessions' as Metric, Paid_sessions as Data)
            
              ,
            
        
                STRUCT('Non_Paid_orders' as Metric, Non_Paid_orders as Data)
            
              ,
            
        
                STRUCT('Non_Paid_sessions' as Metric, Non_Paid_sessions as Data)
            
              ,
            
        
                STRUCT('orders' as Metric, orders as Data)
            
              ,
            
        
                STRUCT('DiscountedSales' as Metric, DiscountedSales as Data)
            
              ,
            
        
                STRUCT('NonDiscountedSales' as Metric, NonDiscountedSales as Data)
            
              ,
            
        
                STRUCT('GrossRevenue' as Metric, GrossRevenue as Data)
            
              ,
            
        
                STRUCT('NetRevenue' as Metric, NetRevenue as Data)
            
              ,
            
        
                STRUCT('MSRP' as Metric, MSRP as Data)
            
              ,
            
        
                STRUCT('ReturnsRevenue' as Metric, ReturnsRevenue as Data)
            
              ,
            
        
                STRUCT('total_product_quantity_auth' as Metric, total_product_quantity_auth as Data)
            
              ,
            
        
                STRUCT('total_product_quantity' as Metric, total_product_quantity as Data)
            
              ,
            
        
                STRUCT('ReturnedUnits' as Metric, ReturnedUnits as Data)
            
              ,
            
        
                STRUCT('PaidNetRevenue' as Metric, PaidNetRevenue as Data)
            
              ,
            
        
                STRUCT('NonPaidNetRevenue' as Metric, NonPaidNetRevenue as Data)
            
              ,
            
        
                STRUCT('Itemscancelled' as Metric, Itemscancelled as Data)
            
              ,
            
        
                STRUCT('Impressions' as Metric, Impressions as Data)
            
              ,
            
        
                STRUCT('Clicks' as Metric, Clicks as Data)
            
              ,
            
        
                STRUCT('ProductDetailViews' as Metric, ProductDetailViews as Data)
            
              ,
            
        
                STRUCT('ProductAddstocart' as Metric, ProductAddstocart as Data)
            
              ,
            
        
                STRUCT('AddstocartPage' as Metric, AddstocartPage as Data)
            
              ,
            
        
                STRUCT('LogginPage' as Metric, LogginPage as Data)
            
              ,
            
        
                STRUCT('ShippingPage' as Metric, ShippingPage as Data)
            
              ,
            
        
                STRUCT('PaymentPage' as Metric, PaymentPage as Data)
            
              ,
            
        
                STRUCT('avgSessionDuration' as Metric, avgSessionDuration as Data)
            
              ,
            
        
                STRUCT('exits' as Metric, exits as Data)
            
              ,
            
        
                STRUCT('H_Sessions' as Metric, H_Sessions as Data)
            
              ,
            
        
                STRUCT('H_AvgsesDur' as Metric, H_AvgsesDur as Data)
            
              ,
            
        
                STRUCT('H_Pages_Per_Sess' as Metric, H_Pages_Per_Sess as Data)
            
              ,
            
        
                STRUCT('H_AvgPgLT' as Metric, H_AvgPgLT as Data)
            
              ,
            
        
                STRUCT('H_Bounces' as Metric, H_Bounces as Data)
            
              ,
            
        
                STRUCT('LP_impressions' as Metric, LP_impressions as Data)
            
              ,
            
        
                STRUCT('LP_Clicks' as Metric, LP_Clicks as Data)
            
              ,
            
        
                STRUCT('LP_addstocart' as Metric, LP_addstocart as Data)
            
              ,
            
        
                STRUCT('LP_Lan_Bounce' as Metric, LP_Lan_Bounce as Data)
            
              ,
            
        
                STRUCT('LP_Lan_Sessions' as Metric, LP_Lan_Sessions as Data)
            
              ,
            
        
                STRUCT('H_Lan_Bounces' as Metric, H_Lan_Bounces as Data)
            
              ,
            
        
                STRUCT('H_Lan_Sessions' as Metric, H_Lan_Sessions as Data)
            
              ,
            
        
                STRUCT('P_bounces' as Metric, P_bounces as Data)
            
              ,
            
        
                STRUCT('P_Sessions' as Metric, P_Sessions as Data)
            
              ,
            
        
                STRUCT('P_Transactions' as Metric, P_Transactions as Data)
            
              ,
            
        
                STRUCT('H_Lan_transactions' as Metric, H_Lan_transactions as Data)
            
              ,
            
        
                STRUCT('LP_Lan_Transactions' as Metric, LP_Lan_Transactions as Data)
            
              ,
            
        
                STRUCT('LP_addstocart' as Metric, LP_addstocart as Data)
            
              ,
            
        
                STRUCT('LP_productDetailViews' as Metric, LP_productDetailViews as Data)
            
              ,
            
        
                STRUCT('tax_amount' as Metric, tax_amount as Data)
            
              ,
            
        
                STRUCT('cancelled_orders' as Metric, cancelled_orders as Data)
            
              ,
            
        
                STRUCT('cod' as Metric, cod as Data)
            
              ,
            
        
                STRUCT('cc' as Metric, cc as Data)
            
              ,
            
        
                STRUCT('returned_orders' as Metric, returned_orders as Data)
            
              ,
            
        
                STRUCT('tabby' as Metric, tabby as Data)
            
              ,
            
        
                STRUCT('pageviews' as Metric, pageviews as Data)
            
              ,
            
        
                STRUCT('mobile' as Metric, mobile as Data)
            
              ,
            
        
                STRUCT('tablet' as Metric, tablet as Data)
            
              ,
            
        
                STRUCT('desktop' as Metric, desktop as Data)
            
              ,
            
        
                STRUCT('newusers_with_age_18_24' as Metric, newusers_with_age_18_24 as Data)
            
              ,
            
        
                STRUCT('newusers_with_age_25_34' as Metric, newusers_with_age_25_34 as Data)
            
              ,
            
        
                STRUCT('newusers_with_age_35_44' as Metric, newusers_with_age_35_44 as Data)
            
              ,
            
        
                STRUCT('newusers_with_age_above_45' as Metric, newusers_with_age_above_45 as Data)
            
              ,
            
        
                STRUCT('newusers_with_gender_female' as Metric, newusers_with_gender_female as Data)
            
              ,
            
        
                STRUCT('newusers_with_gender_male' as Metric, newusers_with_gender_male as Data)
            
              ,
            
        
                STRUCT('new_visitor_sessions' as Metric, new_visitor_sessions as Data)
            
              ,
            
        
                STRUCT('returning_visitor_sessions' as Metric, returning_visitor_sessions as Data)
            
              ,
            
        
                STRUCT('Snapchat_paid_sessions' as Metric, Snapchat_paid_sessions as Data)
            
              ,
            
        
                STRUCT('Snapchat_unpaid_sessions' as Metric, Snapchat_unpaid_sessions as Data)
            
              ,
            
        
                STRUCT('Facebook_Inhouse_Paid_sessions' as Metric, Facebook_Inhouse_Paid_sessions as Data)
            
              ,
            
        
                STRUCT('Facebook_Dynamic_Remarketing_sessions' as Metric, Facebook_Dynamic_Remarketing_sessions as Data)
            
              ,
            
        
                STRUCT('Facebook_Unpaid_sessions' as Metric, Facebook_Unpaid_sessions as Data)
            
              ,
            
        
                STRUCT('Instagram_paid_sessions' as Metric, Instagram_paid_sessions as Data)
            
              ,
            
        
                STRUCT('Instagram_Unpaid_sessions' as Metric, Instagram_Unpaid_sessions as Data)
            
              ,
            
        
                STRUCT('influencersrevenue' as Metric, influencersrevenue as Data)
            
              ,
            
        
                STRUCT('gender_new_users' as Metric, gender_new_users as Data)
            
              ,
            
        
                STRUCT('age_new_users' as Metric, age_new_users as Data)
            
              ,
            
        
                STRUCT('day_level_new_users' as Metric, day_level_new_users as Data)
            
              ,
            
        
                STRUCT('day_level_users' as Metric, day_level_users as Data)
            
              ,
            
        
                STRUCT('SessionDuration' as Metric, SessionDuration as Data)
            
              ,
            
        
                STRUCT('daylevel' as Metric, daylevel as Data)
            
              ,
            
        
                STRUCT('Total_revenue' as Metric, Total_revenue as Data)
            
              ,
            
        
                STRUCT('unsucessful_revenue' as Metric, unsucessful_revenue as Data)
            
              ,
            
        
                STRUCT('influencersorders' as Metric, influencersorders as Data)
            
              ,
            
        
                STRUCT('unsucessful_orders' as Metric, unsucessful_orders as Data)
            
              ,
            
        
                STRUCT('sessions_with_age_18_24' as Metric, sessions_with_age_18_24 as Data)
            
              ,
            
        
                STRUCT('sessions_with_age_25_34' as Metric, sessions_with_age_25_34 as Data)
            
              ,
            
        
                STRUCT('sessions_with_age_35_44' as Metric, sessions_with_age_35_44 as Data)
            
              ,
            
        
                STRUCT('sessions_with_age_above_45' as Metric, sessions_with_age_above_45 as Data)
            
              ,
            
        
                STRUCT('sessions_with_gender_female' as Metric, sessions_with_gender_female as Data)
            
              ,
            
        
                STRUCT('sessions_with_gender_male' as Metric, sessions_with_gender_male as Data)
            
              ,
            
        
                STRUCT('Snapchat_paid_newusers' as Metric, Snapchat_paid_newusers as Data)
            
              ,
            
        
                STRUCT('Snapchat_unpaid_newusers' as Metric, Snapchat_unpaid_newusers as Data)
            
              ,
            
        
                STRUCT('Facebook_Inhouse_Paid_newusers' as Metric, Facebook_Inhouse_Paid_newusers as Data)
            
              ,
            
        
                STRUCT('Facebook_Dynamic_Remarketing_newusers' as Metric, Facebook_Dynamic_Remarketing_newusers as Data)
            
              ,
            
        
                STRUCT('Facebook_Unpaid_newusers' as Metric, Facebook_Unpaid_newusers as Data)
            
              ,
            
        
                STRUCT('Instagram_paid_newusers' as Metric, Instagram_paid_newusers as Data)
            
              ,
            
        
                STRUCT('Instagram_Unpaid_newusers' as Metric, Instagram_Unpaid_newusers as Data)
            
              ,
            
        
                STRUCT('TikTok_paid_newusers' as Metric, TikTok_paid_newusers as Data)
            
              ,
            
        
                STRUCT('newuserssourcemedium' as Metric, newuserssourcemedium as Data)
            
              ,
            
        
                STRUCT('MissL10_spend_day' as Metric, MissL10_spend_day as Data)
            
              ,
            
        
                STRUCT('Husin10_spend_day' as Metric, Husin10_spend_day as Data)
            
              ,
            
        
                STRUCT('age_sessions' as Metric, age_sessions as Data)
            
              ,
            
        
                STRUCT('gender_sessions' as Metric, gender_sessions as Data)
            
              ,
            
        
                STRUCT('Feda10_spend_day' as Metric, Feda10_spend_day as Data)
            
              ,
            
        
                STRUCT('abrar21_spend_day' as Metric, abrar21_spend_day as Data)
            
              ,
            
        
                STRUCT('feda21_spend_day' as Metric, feda21_spend_day as Data)
            
              ,
            
        
                STRUCT('roh_n2_spend_day' as Metric, roh_n2_spend_day as Data)
            
              ,
            
        
                STRUCT('omseba10_spend_day' as Metric, omseba10_spend_day as Data)
            
              ,
            
        
                STRUCT('marwa10_spend_day' as Metric, marwa10_spend_day as Data)
            
              ,
            
        
                STRUCT('maha_140_spend_day' as Metric, maha_140_spend_day as Data)
            
              ,
            
        
                STRUCT('shahad10_spend_day' as Metric, shahad10_spend_day as Data)
            
              ,
            
        
                STRUCT('MissL10_orders' as Metric, MissL10_orders as Data)
            
              ,
            
        
                STRUCT('Husin10_orders' as Metric, Husin10_orders as Data)
            
              ,
            
        
                STRUCT('Feda10_orders' as Metric, Feda10_orders as Data)
            
              ,
            
        
                STRUCT('abrar21_orders' as Metric, abrar21_orders as Data)
            
              ,
            
        
                STRUCT('feda21_orders' as Metric, feda21_orders as Data)
            
              ,
            
        
                STRUCT('roh_n2_orders' as Metric, roh_n2_orders as Data)
            
              ,
            
        
                STRUCT('omseba10_orders' as Metric, omseba10_orders as Data)
            
              ,
            
        
                STRUCT('marwa10_orders' as Metric, marwa10_orders as Data)
            
              ,
            
        
                STRUCT('maha_140_orders' as Metric, maha_140_orders as Data)
            
              ,
            
        
                STRUCT('shahad10_orders' as Metric, shahad10_orders as Data)
            
              ,
            
        
                STRUCT('MissL10_revenue' as Metric, MissL10_revenue as Data)
            
              ,
            
        
                STRUCT('Husin10_revenue' as Metric, Husin10_revenue as Data)
            
              ,
            
        
                STRUCT('Feda10_revenue' as Metric, Feda10_revenue as Data)
            
              ,
            
        
                STRUCT('abrar21_revenue' as Metric, abrar21_revenue as Data)
            
              ,
            
        
                STRUCT('feda21_revenue' as Metric, feda21_revenue as Data)
            
              ,
            
        
                STRUCT('roh_n2_revenue' as Metric, roh_n2_revenue as Data)
            
              ,
            
        
                STRUCT('omseba10_revenue' as Metric, omseba10_revenue as Data)
            
              ,
            
        
                STRUCT('marwa10_revenue' as Metric, marwa10_revenue as Data)
            
              ,
            
        
                STRUCT('maha_140_revenue' as Metric, maha_140_revenue as Data)
            
              ,
            
        
                STRUCT('shahad10_revenue' as Metric, shahad10_revenue as Data)
            
              ,
            
        
                STRUCT('Facebook_cust_traffic' as Metric, Facebook_cust_traffic as Data)
            
              ,
            
        
                STRUCT('Instagram_cust_traffic' as Metric, Instagram_cust_traffic as Data)
            
              ,
            
        
                STRUCT('Snapchat_cust_traffic' as Metric, Snapchat_cust_traffic as Data)
            
              ,
            
        
                STRUCT('Adwords_cust_traffic' as Metric, Adwords_cust_traffic as Data)
            
              ,
            
        
                STRUCT('Roi_hunter_cust_traffic' as Metric, Roi_hunter_cust_traffic as Data)
            
              ,
            
        
                STRUCT('Facebook_cust_orders' as Metric, Facebook_cust_orders as Data)
            
              ,
            
        
                STRUCT('Instagram_cust_orders' as Metric, Instagram_cust_orders as Data)
            
              ,
            
        
                STRUCT('Snapchat_cust_orders' as Metric, Snapchat_cust_orders as Data)
            
              ,
            
        
                STRUCT('Adwords_cust_orders' as Metric, Adwords_cust_orders as Data)
            
              ,
            
        
                STRUCT('Roi_hunter_cust_orders' as Metric, Roi_hunter_cust_orders as Data)
            
              ,
            
        
                STRUCT('Facebook_cust_revenue' as Metric, Facebook_cust_revenue as Data)
            
              ,
            
        
                STRUCT('Instagram_cust_revenue' as Metric, Instagram_cust_revenue as Data)
            
              ,
            
        
                STRUCT('Snapchat_cust_revenue' as Metric, Snapchat_cust_revenue as Data)
            
              ,
            
        
                STRUCT('Adwords_cust_revenue' as Metric, Adwords_cust_revenue as Data)
            
              ,
            
        
                STRUCT('Roi_hunter_cust_revenue' as Metric, Roi_hunter_cust_revenue as Data)
            
              ,
            
        
                STRUCT('Facebook_cust_spend' as Metric, Facebook_cust_spend as Data)
            
              ,
            
        
                STRUCT('google_cust_adspend' as Metric, google_cust_adspend as Data)
            
              ,
            
        
                STRUCT('google_cust_revenue' as Metric, google_cust_revenue as Data)
            
              ,
            
        
                STRUCT('Instagram_cust_spend' as Metric, Instagram_cust_spend as Data)
            
              ,
            
        
                STRUCT('roi_hunter_cust_spend' as Metric, roi_hunter_cust_spend as Data)
            
              ,
            
        
                STRUCT('snapchat_cust_spend' as Metric, snapchat_cust_spend as Data)
            
              ,
            
        
                STRUCT('TikTok_paid_sessions' as Metric, TikTok_paid_sessions as Data)
            
              ,
            
        
                STRUCT('TikTok_Unpaid_sessions' as Metric, TikTok_Unpaid_sessions as Data)
            
              ,
            
        
                STRUCT('Other_Social_sessions' as Metric, Other_Social_sessions as Data)
            
              ,
            
        
                STRUCT('Paid_Search_sessions' as Metric, Paid_Search_sessions as Data)
            
              ,
            
        
                STRUCT('Internal_Referral_sessions' as Metric, Internal_Referral_sessions as Data)
            
              ,
            
        
                STRUCT('Email_sessions' as Metric, Email_sessions as Data)
            
              ,
            
        
                STRUCT('Organic_Search_Sessions' as Metric, Organic_Search_Sessions as Data)
            
              ,
            
        
                STRUCT('Referral_Sessions' as Metric, Referral_Sessions as Data)
            
              ,
            
        
                STRUCT('Direct_Sessions' as Metric, Direct_Sessions as Data)
            
              ,
            
        
                STRUCT('Others_Sessions' as Metric, Others_Sessions as Data)
            
              ,
            
        
                STRUCT('TikTok_Unpaid_newusers' as Metric, TikTok_Unpaid_newusers as Data)
            
              ,
            
        
                STRUCT('Other_Social_newusers' as Metric, Other_Social_newusers as Data)
            
              ,
            
        
                STRUCT('Paid_Search_newusers' as Metric, Paid_Search_newusers as Data)
            
              ,
            
        
                STRUCT('Internal_Referral_newusers' as Metric, Internal_Referral_newusers as Data)
            
              ,
            
        
                STRUCT('Email_newusers' as Metric, Email_newusers as Data)
            
              ,
            
        
                STRUCT('Organic_Search_newusers' as Metric, Organic_Search_newusers as Data)
            
              ,
            
        
                STRUCT('Referral_newusers' as Metric, Referral_newusers as Data)
            
              ,
            
        
                STRUCT('Direct_newusers' as Metric, Direct_newusers as Data)
            
              ,
            
        
                STRUCT('Others_newusers' as Metric, Others_newusers as Data)
            
              ,
            
        
                STRUCT('Rand10_spend_day' as Metric, Rand10_spend_day as Data)
            
              ,
            
        
                STRUCT('Satha10_spend_day' as Metric, Satha10_spend_day as Data)
            
              ,
            
        
                STRUCT('Wish10_spend_day' as Metric, Wish10_spend_day as Data)
            
              ,
            
        
                STRUCT('Rand10_orders' as Metric, Rand10_orders as Data)
            
              ,
            
        
                STRUCT('Satha10_orders' as Metric, Satha10_orders as Data)
            
              ,
            
        
                STRUCT('Wish10_orders' as Metric, Wish10_orders as Data)
            
              ,
            
        
                STRUCT('Rand10_revenue' as Metric, Rand10_revenue as Data)
            
              ,
            
        
                STRUCT('Satha10_revenue' as Metric, Satha10_revenue as Data)
            
              ,
            
        
                STRUCT('Wish10_revenue' as Metric, Wish10_revenue as Data)
            
              ,
            
        
                STRUCT('target' as Metric, target as Data)
            
              ,
            
        
                STRUCT('total_time_to_confirm' as Metric, total_time_to_confirm as Data)
            
              ,
            
        
                STRUCT('total_time_to_ship' as Metric, total_time_to_ship as Data)
            
              ,
            
        
                STRUCT('total_time_to_deliver' as Metric, total_time_to_deliver as Data)
            
              ,
            
        
                STRUCT('total_time_to_fulfill' as Metric, total_time_to_fulfill as Data)
            
              ,
            
        
                STRUCT('orders_created' as Metric, orders_created as Data)
            
              ,
            
        
                STRUCT('orders_confirmed' as Metric, orders_confirmed as Data)
            
              ,
            
        
                STRUCT('orders_shipped' as Metric, orders_shipped as Data)
            
              ,
            
        
                STRUCT('orders_delivered' as Metric, orders_delivered as Data)
            
              ,
            
        
                STRUCT('Yasmine10_spend_day' as Metric, Yasmine10_spend_day as Data)
            
              ,
            
        
                STRUCT('Yasmine10_orders' as Metric, Yasmine10_orders as Data)
            
              ,
            
        
                STRUCT('Yasmine10_revenue' as Metric, Yasmine10_revenue as Data)
            
              ,
            
        
                STRUCT('Fatma10_spend_day' as Metric, Fatma10_spend_day as Data)
            
              ,
            
        
                STRUCT('Fatma10_orders' as Metric, Fatma10_orders as Data)
            
              ,
            
        
                STRUCT('Fatma10_revenue' as Metric, Fatma10_revenue as Data)
            
              ,
            
        
                STRUCT('target_spend' as Metric, target_spend as Data)
            
              ,
            
        
                STRUCT('facebook_target_day' as Metric, facebook_target_day as Data)
            
              ,
            
        
                STRUCT('adwords_target_day' as Metric, adwords_target_day as Data)
            
              ,
            
        
                STRUCT('snapchat_target_day' as Metric, snapchat_target_day as Data)
            
              ,
            
        
                STRUCT('on_time_shipments' as Metric, on_time_shipments as Data)
            
              ,
            
        
                STRUCT('delayed_shipments' as Metric, delayed_shipments as Data)
            
              ,
            
        
                STRUCT('refused_shipments' as Metric, refused_shipments as Data)
            
              ,
            
        
                STRUCT('total_attempts' as Metric, total_attempts as Data)
            
              ,
            
        
                STRUCT('total_tickets_created' as Metric, total_tickets_created as Data)
            
              ,
            
        
                STRUCT('facebook_tickets' as Metric, facebook_tickets as Data)
            
              ,
            
        
                STRUCT('chat_tickets' as Metric, chat_tickets as Data)
            
              ,
            
        
                STRUCT('email_tickets' as Metric, email_tickets as Data)
            
              ,
            
        
                STRUCT('twitter_tickets' as Metric, twitter_tickets as Data)
            
              ,
            
        
                STRUCT('web_tickets' as Metric, web_tickets as Data)
            
              ,
            
        
                STRUCT('other_cases' as Metric, other_cases as Data)
            
              ,
            
        
                STRUCT('total_days_to_close' as Metric, total_days_to_close as Data)
            
              ,
            
        
                STRUCT('new_tickets' as Metric, new_tickets as Data)
            
              ,
            
        
                STRUCT('open_tickets' as Metric, open_tickets as Data)
            
              ,
            
        
                STRUCT('closed_tickets' as Metric, closed_tickets as Data)
            
              ,
            
        
                STRUCT('pending_tickets' as Metric, pending_tickets as Data)
            
              ,
            
        
                STRUCT('solved_tickets' as Metric, solved_tickets as Data)
            
              ,
            
        
                STRUCT('deleted_tickets' as Metric, deleted_tickets as Data)
            
              ,
            
        
                STRUCT('general_query' as Metric, general_query as Data)
            
              ,
            
        
                STRUCT('return' as Metric, return as Data)
            
              ,
            
        
                STRUCT('exchange' as Metric, exchange as Data)
            
              ,
            
        
                STRUCT('order_delay' as Metric, order_delay as Data)
            
              ,
            
        
                STRUCT('wrong_order' as Metric, wrong_order as Data)
            
              ,
            
        
                STRUCT('manufacturing' as Metric, manufacturing as Data)
            
              ,
            
        
                STRUCT('website_issue' as Metric, website_issue as Data)
            
              ,
            
        
                STRUCT('shipments' as Metric, shipments as Data)
            
              ,
            
        
                STRUCT('missl2u_spend_day' as Metric, missl2u_spend_day as Data)
            
              ,
            
        
                STRUCT('missl2u_orders' as Metric, missl2u_orders as Data)
            
              ,
            
        
                STRUCT('missl2u_revenue' as Metric, missl2u_revenue as Data)
            
              ,
            
        
                STRUCT('Existing_Customers' as Metric, Existing_Customers as Data)
            
              ,
            
        
                STRUCT('Youtube_cust_traffic' as Metric, Youtube_cust_traffic as Data)
            
              ,
            
        
                STRUCT('Tiktok_cust_traffic' as Metric, Tiktok_cust_traffic as Data)
            
              ,
            
        
                STRUCT('Youtube_cust_orders' as Metric, Youtube_cust_orders as Data)
            
              ,
            
        
                STRUCT('Tiktok_cust_orders' as Metric, Tiktok_cust_orders as Data)
            
              ,
            
        
                STRUCT('Tiktok_cust_revenue' as Metric, Tiktok_cust_revenue as Data)
            
              ,
            
        
                STRUCT('Youtube_cust_revenue' as Metric, Youtube_cust_revenue as Data)
            
              ,
            
        
                STRUCT('spend_per_day' as Metric, spend_per_day as Data)
            
              ,
            
        
                STRUCT('sku_more_than_six' as Metric, sku_more_than_six as Data)
            
              ,
            
        
                STRUCT('sku_more_than_three' as Metric, sku_more_than_three as Data)
            
              ,
            
        
                STRUCT('sku_less_than_three' as Metric, sku_less_than_three as Data)
            
              ,
            
        
                STRUCT('num_of_skus' as Metric, num_of_skus as Data)
            
              ,
            
        
                STRUCT('skus_80_20' as Metric, skus_80_20 as Data)
            
              ,
            
        
                STRUCT('no_launched' as Metric, no_launched as Data)
            
              ,
            
        
                STRUCT('tiktok_target_day' as Metric, tiktok_target_day as Data)
            
              ,
            
        
                STRUCT('tiktok_cust_spend' as Metric, tiktok_cust_spend as Data)
            
              ,
            
        
                STRUCT('youtube_target_day' as Metric, youtube_target_day as Data)
            
        
        ] Metrics_data
        from (
        select * from `noted-computing-279322.halo_1_1.fDailyAgg`)
        )
cross join
unnest(Metrics_data) as Metrics_data
)) ta
group by rollup(Halo_country, KPI, dateyear, datequarter, datemonth, dateweek, date)
) where Halo_country is not null and KPI is not null
and coalesce(date,dateweek,datemonth,datequarter,dateyear) is not null
) where week_rno = 1
) ta,
`noted-computing-279322.halo_1_1.refKPI` tb
where ta.KPI = tb.KPI_VARIABLE

