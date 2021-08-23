
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
union all
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
'AOV' KPI,
date, XTD,
ifnull(safe_divide(total_item_price_auth,orders_auth),0) value,
ifnull(safe_divide(total_item_price_auth_prevX,orders_auth_prevX),0) prevvalue,
ifnull(safe_divide(total_item_price_auth_PrevyearsameX,orders_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
                ,
        
        
           max(case when KPI='orders_auth' then Value else null end ) orders_auth,
           max(case when KPI='orders_auth' then prevValue else null end ) orders_auth_PrevX,
           max(case when KPI='orders_auth' then prevyearvalue else null end ) orders_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'conversion_rate_per' KPI,
date, XTD,
ifnull(safe_divide(orders_auth,sessions),0) value,
ifnull(safe_divide(orders_auth_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(orders_auth_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='orders_auth' then Value else null end ) orders_auth,
           max(case when KPI='orders_auth' then prevValue else null end ) orders_auth_PrevX,
           max(case when KPI='orders_auth' then prevyearvalue else null end ) orders_auth_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ROAS' KPI,
date, XTD,
ifnull(safe_divide(total_ad_revenue_auth,spend),0) value,
ifnull(safe_divide(total_ad_revenue_auth_prevX,spend_prevX),0) prevvalue,
ifnull(safe_divide(total_ad_revenue_auth_PrevyearsameX,spend_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='total_ad_revenue_auth' then Value else null end ) total_ad_revenue_auth,
           max(case when KPI='total_ad_revenue_auth' then prevValue else null end ) total_ad_revenue_auth_PrevX,
           max(case when KPI='total_ad_revenue_auth' then prevyearvalue else null end ) total_ad_revenue_auth_PrevyearsameX
       
                ,
        
        
           max(case when KPI='spend' then Value else null end ) spend,
           max(case when KPI='spend' then prevValue else null end ) spend_PrevX,
           max(case when KPI='spend' then prevyearvalue else null end ) spend_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'CAC' KPI,
date, XTD,
ifnull(safe_divide(spend,New_customers),0) value,
ifnull(safe_divide(Spend_prevX,New_customers_prevX),0) prevvalue,
ifnull(safe_divide(Spend_PrevyearsameX,New_customers_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='spend' then Value else null end ) spend,
           max(case when KPI='spend' then prevValue else null end ) spend_PrevX,
           max(case when KPI='spend' then prevyearvalue else null end ) spend_PrevyearsameX
       
                ,
        
        
           max(case when KPI='New_customers' then Value else null end ) New_customers,
           max(case when KPI='New_customers' then prevValue else null end ) New_customers_PrevX,
           max(case when KPI='New_customers' then prevyearvalue else null end ) New_customers_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'productviews_cart_per' KPI,
date, XTD,
ifnull(safe_divide(Addstocart,Productviews),0) value,
ifnull(safe_divide(Addstocart_prevX,Productviews_prevX),0) prevvalue,
ifnull(safe_divide(Addstocart_PrevyearsameX,Productviews_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Addstocart' then Value else null end ) Addstocart,
           max(case when KPI='Addstocart' then prevValue else null end ) Addstocart_PrevX,
           max(case when KPI='Addstocart' then prevyearvalue else null end ) Addstocart_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Productviews' then Value else null end ) Productviews,
           max(case when KPI='Productviews' then prevValue else null end ) Productviews_PrevX,
           max(case when KPI='Productviews' then prevyearvalue else null end ) Productviews_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'cart_checkout_per' KPI,
date, XTD,
ifnull(safe_divide(Checkout,Addstocart),0) value,
ifnull(safe_divide(Checkout_prevX,Addstocart_prevX),0) prevvalue,
ifnull(safe_divide(Checkout_PrevyearsameX,Addstocart_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Checkout' then Value else null end ) Checkout,
           max(case when KPI='Checkout' then prevValue else null end ) Checkout_PrevX,
           max(case when KPI='Checkout' then prevyearvalue else null end ) Checkout_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Addstocart' then Value else null end ) Addstocart,
           max(case when KPI='Addstocart' then prevValue else null end ) Addstocart_PrevX,
           max(case when KPI='Addstocart' then prevyearvalue else null end ) Addstocart_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'checkout_conversion_per' KPI,
date, XTD,
ifnull(safe_divide(Transactions,Checkout),0) value,
ifnull(safe_divide(Transactions_prevX,Checkout_prevX),0) prevvalue,
ifnull(safe_divide(Transactions_PrevyearsameX,Checkout_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Transactions' then Value else null end ) Transactions,
           max(case when KPI='Transactions' then prevValue else null end ) Transactions_PrevX,
           max(case when KPI='Transactions' then prevyearvalue else null end ) Transactions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Checkout' then Value else null end ) Checkout,
           max(case when KPI='Checkout' then prevValue else null end ) Checkout_PrevX,
           max(case when KPI='Checkout' then prevyearvalue else null end ) Checkout_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'login_shipping_per' KPI,
date, XTD,
ifnull(safe_divide(Shipping,Checkout),0) value,
ifnull(safe_divide(Shipping_prevX,Checkout_prevX),0) prevvalue,
ifnull(safe_divide(Shipping_PrevyearsameX,Checkout_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Shipping' then Value else null end ) Shipping,
           max(case when KPI='Shipping' then prevValue else null end ) Shipping_PrevX,
           max(case when KPI='Shipping' then prevyearvalue else null end ) Shipping_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Checkout' then Value else null end ) Checkout,
           max(case when KPI='Checkout' then prevValue else null end ) Checkout_PrevX,
           max(case when KPI='Checkout' then prevyearvalue else null end ) Checkout_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'shipping_payment_per' KPI,
date, XTD,
ifnull(safe_divide(PaymentClick,Shipping),0) value,
ifnull(safe_divide(PaymentClick_prevX,Shipping_prevX),0) prevvalue,
ifnull(safe_divide(PaymentClick_PrevyearsameX,Shipping_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='PaymentClick' then Value else null end ) PaymentClick,
           max(case when KPI='PaymentClick' then prevValue else null end ) PaymentClick_PrevX,
           max(case when KPI='PaymentClick' then prevyearvalue else null end ) PaymentClick_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Shipping' then Value else null end ) Shipping,
           max(case when KPI='Shipping' then prevValue else null end ) Shipping_PrevX,
           max(case when KPI='Shipping' then prevyearvalue else null end ) Shipping_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'payment_txn_per' KPI,
date, XTD,
ifnull(safe_divide(Transactions,PaymentClick),0) value,
ifnull(safe_divide(Transactions_prevX,PaymentClick_prevX),0) prevvalue,
ifnull(safe_divide(Transactions_PrevyearsameX,PaymentClick_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Transactions' then Value else null end ) Transactions,
           max(case when KPI='Transactions' then prevValue else null end ) Transactions_PrevX,
           max(case when KPI='Transactions' then prevyearvalue else null end ) Transactions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='PaymentClick' then Value else null end ) PaymentClick,
           max(case when KPI='PaymentClick' then prevValue else null end ) PaymentClick_PrevX,
           max(case when KPI='PaymentClick' then prevyearvalue else null end ) PaymentClick_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'bounce_rate_per' KPI,
date, XTD,
ifnull(safe_divide(Bounce,sessions),0) value,
ifnull(safe_divide(Bounce_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(Bounce_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Bounce' then Value else null end ) Bounce,
           max(case when KPI='Bounce' then prevValue else null end ) Bounce_PrevX,
           max(case when KPI='Bounce' then prevyearvalue else null end ) Bounce_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'paid_conversion_rate_per' KPI,
date, XTD,
ifnull(safe_divide(Paid_orders,Paid_sessions),0) value,
ifnull(safe_divide(Paid_orders_prevX,Paid_sessions_prevX),0) prevvalue,
ifnull(safe_divide(Paid_orders_PrevyearsameX,Paid_sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Paid_orders' then Value else null end ) Paid_orders,
           max(case when KPI='Paid_orders' then prevValue else null end ) Paid_orders_PrevX,
           max(case when KPI='Paid_orders' then prevyearvalue else null end ) Paid_orders_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Paid_sessions' then Value else null end ) Paid_sessions,
           max(case when KPI='Paid_sessions' then prevValue else null end ) Paid_sessions_PrevX,
           max(case when KPI='Paid_sessions' then prevyearvalue else null end ) Paid_sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'unpaid_conversion_rate_per' KPI,
date, XTD,
ifnull(safe_divide(Non_Paid_orders,Non_Paid_sessions),0) value,
ifnull(safe_divide(Non_Paid_orders_prevX,Non_Paid_sessions_prevX),0) prevvalue,
ifnull(safe_divide(Non_Paid_orders_PrevyearsameX,Non_Paid_sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Non_Paid_orders' then Value else null end ) Non_Paid_orders,
           max(case when KPI='Non_Paid_orders' then prevValue else null end ) Non_Paid_orders_PrevX,
           max(case when KPI='Non_Paid_orders' then prevyearvalue else null end ) Non_Paid_orders_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Non_Paid_sessions' then Value else null end ) Non_Paid_sessions,
           max(case when KPI='Non_Paid_sessions' then prevValue else null end ) Non_Paid_sessions_PrevX,
           max(case when KPI='Non_Paid_sessions' then prevyearvalue else null end ) Non_Paid_sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'margin_per' KPI,
date, XTD,
ifnull(safe_divide(DiscountedSales,NonDiscountedSales),0) value,
ifnull(safe_divide(DiscountedSales_prevX,NonDiscountedSales_prevX),0) prevvalue,
ifnull(safe_divide(DiscountedSales_PrevyearsameX,NonDiscountedSales_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='DiscountedSales' then Value else null end ) DiscountedSales,
           max(case when KPI='DiscountedSales' then prevValue else null end ) DiscountedSales_PrevX,
           max(case when KPI='DiscountedSales' then prevyearvalue else null end ) DiscountedSales_PrevyearsameX
       
                ,
        
        
           max(case when KPI='NonDiscountedSales' then Value else null end ) NonDiscountedSales,
           max(case when KPI='NonDiscountedSales' then prevValue else null end ) NonDiscountedSales_PrevX,
           max(case when KPI='NonDiscountedSales' then prevyearvalue else null end ) NonDiscountedSales_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'OtherOrders' KPI,
date, XTD,
ifnull(OrdersAllstatus-Transactions,0) value,
ifnull(OrdersAllstatus_prevX-Transactions_prevX,0) prevvalue,
ifnull(OrdersAllstatus_PrevyearsameX-Transactions_PrevyearsameX,0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='OrdersAllstatus' then Value else null end ) OrdersAllstatus,
           max(case when KPI='OrdersAllstatus' then prevValue else null end ) OrdersAllstatus_PrevX,
           max(case when KPI='OrdersAllstatus' then prevyearvalue else null end ) OrdersAllstatus_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Transactions' then Value else null end ) Transactions,
           max(case when KPI='Transactions' then prevValue else null end ) Transactions_PrevX,
           max(case when KPI='Transactions' then prevyearvalue else null end ) Transactions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'AOV_F' KPI,
date, XTD,
ifnull(safe_divide(Revenue_F,Transactions_F),0) value,
ifnull(safe_divide(Revenue_F_prevX,Transactions_F_prevX),0) prevvalue,
ifnull(safe_divide(Revenue_F_PrevyearsameX,Transactions_F_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Revenue_F' then Value else null end ) Revenue_F,
           max(case when KPI='Revenue_F' then prevValue else null end ) Revenue_F_PrevX,
           max(case when KPI='Revenue_F' then prevyearvalue else null end ) Revenue_F_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Transactions_F' then Value else null end ) Transactions_F,
           max(case when KPI='Transactions_F' then prevValue else null end ) Transactions_F_PrevX,
           max(case when KPI='Transactions_F' then prevyearvalue else null end ) Transactions_F_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'conversion_rate_F_per' KPI,
date, XTD,
ifnull(safe_divide(Transactions_F,Sessions_F),0) value,
ifnull(safe_divide(Transactions_F_prevX,Sessions_F_prevX),0) prevvalue,
ifnull(safe_divide(Transactions_F_PrevyearsameX,Sessions_F_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Transactions_F' then Value else null end ) Transactions_F,
           max(case when KPI='Transactions_F' then prevValue else null end ) Transactions_F_PrevX,
           max(case when KPI='Transactions_F' then prevyearvalue else null end ) Transactions_F_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Sessions_F' then Value else null end ) Sessions_F,
           max(case when KPI='Sessions_F' then prevValue else null end ) Sessions_F_PrevX,
           max(case when KPI='Sessions_F' then prevyearvalue else null end ) Sessions_F_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ROAS_F' KPI,
date, XTD,
ifnull(safe_divide(Revenue_F,Spend_F),0) value,
ifnull(safe_divide(Revenue_F_prevX,Spend_F_prevX),0) prevvalue,
ifnull(safe_divide(Revenue_F_PrevyearsameX,Spend_F_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Revenue_F' then Value else null end ) Revenue_F,
           max(case when KPI='Revenue_F' then prevValue else null end ) Revenue_F_PrevX,
           max(case when KPI='Revenue_F' then prevyearvalue else null end ) Revenue_F_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Spend_F' then Value else null end ) Spend_F,
           max(case when KPI='Spend_F' then prevValue else null end ) Spend_F_PrevX,
           max(case when KPI='Spend_F' then prevyearvalue else null end ) Spend_F_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'CTR_F_per' KPI,
date, XTD,
ifnull(safe_divide(Adclicks_F,Impressions_F),0) value,
ifnull(safe_divide(Adclicks_F_prevX,Impressions_F_prevX),0) prevvalue,
ifnull(safe_divide(Adclicks_F_PrevyearsameX,Impressions_F_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Adclicks_F' then Value else null end ) Adclicks_F,
           max(case when KPI='Adclicks_F' then prevValue else null end ) Adclicks_F_PrevX,
           max(case when KPI='Adclicks_F' then prevyearvalue else null end ) Adclicks_F_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Impressions_F' then Value else null end ) Impressions_F,
           max(case when KPI='Impressions_F' then prevValue else null end ) Impressions_F_PrevX,
           max(case when KPI='Impressions_F' then prevyearvalue else null end ) Impressions_F_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'add_to_cart_ratio_F_per' KPI,
date, XTD,
ifnull(safe_divide(Addtocarts_F,Sessions_F),0) value,
ifnull(safe_divide(Addtocarts_F_prevX,Sessions_F_prevX),0) prevvalue,
ifnull(safe_divide(Addtocarts_F_PrevyearsameX,Sessions_F_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Addtocarts_F' then Value else null end ) Addtocarts_F,
           max(case when KPI='Addtocarts_F' then prevValue else null end ) Addtocarts_F_PrevX,
           max(case when KPI='Addtocarts_F' then prevyearvalue else null end ) Addtocarts_F_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Sessions_F' then Value else null end ) Sessions_F,
           max(case when KPI='Sessions_F' then prevValue else null end ) Sessions_F_PrevX,
           max(case when KPI='Sessions_F' then prevyearvalue else null end ) Sessions_F_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'cost_per_click_F' KPI,
date, XTD,
ifnull(safe_divide(Spend_F,Adclicks_F),0) value,
ifnull(safe_divide(Spend_F_prevX,Adclicks_F_prevX),0) prevvalue,
ifnull(safe_divide(Spend_F_PrevyearsameX,Adclicks_F_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Spend_F' then Value else null end ) Spend_F,
           max(case when KPI='Spend_F' then prevValue else null end ) Spend_F_PrevX,
           max(case when KPI='Spend_F' then prevyearvalue else null end ) Spend_F_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Adclicks_F' then Value else null end ) Adclicks_F,
           max(case when KPI='Adclicks_F' then prevValue else null end ) Adclicks_F_PrevX,
           max(case when KPI='Adclicks_F' then prevyearvalue else null end ) Adclicks_F_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'CostPerSession_F' KPI,
date, XTD,
ifnull(safe_divide(Spend_F,Sessions_F),0) value,
ifnull(safe_divide(Spend_F_prevX,Sessions_F_prevX),0) prevvalue,
ifnull(safe_divide(Spend_F_PrevyearsameX,Sessions_F_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Spend_F' then Value else null end ) Spend_F,
           max(case when KPI='Spend_F' then prevValue else null end ) Spend_F_PrevX,
           max(case when KPI='Spend_F' then prevyearvalue else null end ) Spend_F_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Sessions_F' then Value else null end ) Sessions_F,
           max(case when KPI='Sessions_F' then prevValue else null end ) Sessions_F_PrevX,
           max(case when KPI='Sessions_F' then prevyearvalue else null end ) Sessions_F_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'CostPerTransaction_F' KPI,
date, XTD,
ifnull(safe_divide(Spend_F,Transactions_F),0) value,
ifnull(safe_divide(Spend_F_prevX,Transactions_F_prevX),0) prevvalue,
ifnull(safe_divide(Spend_F_PrevyearsameX,Transactions_F_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Spend_F' then Value else null end ) Spend_F,
           max(case when KPI='Spend_F' then prevValue else null end ) Spend_F_PrevX,
           max(case when KPI='Spend_F' then prevyearvalue else null end ) Spend_F_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Transactions_F' then Value else null end ) Transactions_F,
           max(case when KPI='Transactions_F' then prevValue else null end ) Transactions_F_PrevX,
           max(case when KPI='Transactions_F' then prevyearvalue else null end ) Transactions_F_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'bounce_rate_F_per' KPI,
date, XTD,
ifnull(safe_divide(Bounces_F,Sessions_F),0) value,
ifnull(safe_divide(Bounces_F_prevX,Sessions_F_prevX),0) prevvalue,
ifnull(safe_divide(Bounces_F_PrevyearsameX,Sessions_F_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Bounces_F' then Value else null end ) Bounces_F,
           max(case when KPI='Bounces_F' then prevValue else null end ) Bounces_F_PrevX,
           max(case when KPI='Bounces_F' then prevyearvalue else null end ) Bounces_F_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Sessions_F' then Value else null end ) Sessions_F,
           max(case when KPI='Sessions_F' then prevValue else null end ) Sessions_F_PrevX,
           max(case when KPI='Sessions_F' then prevyearvalue else null end ) Sessions_F_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'AOV_G' KPI,
date, XTD,
ifnull(safe_divide(Revenue_G,Transactions_G),0) value,
ifnull(safe_divide(Revenue_G_prevX,Transactions_G_prevX),0) prevvalue,
ifnull(safe_divide(Revenue_G_PrevyearsameX,Transactions_G_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Revenue_G' then Value else null end ) Revenue_G,
           max(case when KPI='Revenue_G' then prevValue else null end ) Revenue_G_PrevX,
           max(case when KPI='Revenue_G' then prevyearvalue else null end ) Revenue_G_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Transactions_G' then Value else null end ) Transactions_G,
           max(case when KPI='Transactions_G' then prevValue else null end ) Transactions_G_PrevX,
           max(case when KPI='Transactions_G' then prevyearvalue else null end ) Transactions_G_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'conversion_rate_G_per' KPI,
date, XTD,
ifnull(safe_divide(Transactions_G,Sessions_G),0) value,
ifnull(safe_divide(Transactions_G_prevX,Sessions_G_prevX),0) prevvalue,
ifnull(safe_divide(Transactions_G_PrevyearsameX,Sessions_G_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Transactions_G' then Value else null end ) Transactions_G,
           max(case when KPI='Transactions_G' then prevValue else null end ) Transactions_G_PrevX,
           max(case when KPI='Transactions_G' then prevyearvalue else null end ) Transactions_G_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Sessions_G' then Value else null end ) Sessions_G,
           max(case when KPI='Sessions_G' then prevValue else null end ) Sessions_G_PrevX,
           max(case when KPI='Sessions_G' then prevyearvalue else null end ) Sessions_G_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ROAS_G' KPI,
date, XTD,
ifnull(safe_divide(google_cust_revenue,google_cust_spend),0) value,
ifnull(safe_divide(google_cust_revenue_prevX,google_cust_spend_prevX),0) prevvalue,
ifnull(safe_divide(google_cust_revenue_PrevyearsameX,google_cust_spend_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='google_cust_revenue' then Value else null end ) google_cust_revenue,
           max(case when KPI='google_cust_revenue' then prevValue else null end ) google_cust_revenue_PrevX,
           max(case when KPI='google_cust_revenue' then prevyearvalue else null end ) google_cust_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='google_cust_spend' then Value else null end ) google_cust_spend,
           max(case when KPI='google_cust_spend' then prevValue else null end ) google_cust_spend_PrevX,
           max(case when KPI='google_cust_spend' then prevyearvalue else null end ) google_cust_spend_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'CTR_G_PER' KPI,
date, XTD,
ifnull(safe_divide(Adclicks_G,Impressions_G),0) value,
ifnull(safe_divide(Adclicks_G_prevX,Impressions_G_prevX),0) prevvalue,
ifnull(safe_divide(Adclicks_G_PrevyearsameX,Impressions_G_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Adclicks_G' then Value else null end ) Adclicks_G,
           max(case when KPI='Adclicks_G' then prevValue else null end ) Adclicks_G_PrevX,
           max(case when KPI='Adclicks_G' then prevyearvalue else null end ) Adclicks_G_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Impressions_G' then Value else null end ) Impressions_G,
           max(case when KPI='Impressions_G' then prevValue else null end ) Impressions_G_PrevX,
           max(case when KPI='Impressions_G' then prevyearvalue else null end ) Impressions_G_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'add_to_cart_ratio_G_per' KPI,
date, XTD,
ifnull(safe_divide(Addtocarts_G,Sessions_G),0) value,
ifnull(safe_divide(Addtocarts_G_prevX,Sessions_G_prevX),0) prevvalue,
ifnull(safe_divide(Addtocarts_G_PrevyearsameX,Sessions_G_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Addtocarts_G' then Value else null end ) Addtocarts_G,
           max(case when KPI='Addtocarts_G' then prevValue else null end ) Addtocarts_G_PrevX,
           max(case when KPI='Addtocarts_G' then prevyearvalue else null end ) Addtocarts_G_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Sessions_G' then Value else null end ) Sessions_G,
           max(case when KPI='Sessions_G' then prevValue else null end ) Sessions_G_PrevX,
           max(case when KPI='Sessions_G' then prevyearvalue else null end ) Sessions_G_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'CostPerClick_G' KPI,
date, XTD,
ifnull(safe_divide(Spend_G,Adclicks_G),0) value,
ifnull(safe_divide(Spend_G_prevX,Adclicks_G_prevX),0) prevvalue,
ifnull(safe_divide(Spend_G_PrevyearsameX,Adclicks_G_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Spend_G' then Value else null end ) Spend_G,
           max(case when KPI='Spend_G' then prevValue else null end ) Spend_G_PrevX,
           max(case when KPI='Spend_G' then prevyearvalue else null end ) Spend_G_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Adclicks_G' then Value else null end ) Adclicks_G,
           max(case when KPI='Adclicks_G' then prevValue else null end ) Adclicks_G_PrevX,
           max(case when KPI='Adclicks_G' then prevyearvalue else null end ) Adclicks_G_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'CostPerSession_G' KPI,
date, XTD,
ifnull(safe_divide(Spend_G,Sessions_G),0) value,
ifnull(safe_divide(Spend_G_prevX,Sessions_G_prevX),0) prevvalue,
ifnull(safe_divide(Spend_G_PrevyearsameX,Sessions_G_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Spend_G' then Value else null end ) Spend_G,
           max(case when KPI='Spend_G' then prevValue else null end ) Spend_G_PrevX,
           max(case when KPI='Spend_G' then prevyearvalue else null end ) Spend_G_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Sessions_G' then Value else null end ) Sessions_G,
           max(case when KPI='Sessions_G' then prevValue else null end ) Sessions_G_PrevX,
           max(case when KPI='Sessions_G' then prevyearvalue else null end ) Sessions_G_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'CostPerTransaction_G' KPI,
date, XTD,
ifnull(safe_divide(Spend_G,Transactions_G),0) value,
ifnull(safe_divide(Spend_G_prevX,Transactions_G_prevX),0) prevvalue,
ifnull(safe_divide(Spend_G_PrevyearsameX,Transactions_G_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Spend_G' then Value else null end ) Spend_G,
           max(case when KPI='Spend_G' then prevValue else null end ) Spend_G_PrevX,
           max(case when KPI='Spend_G' then prevyearvalue else null end ) Spend_G_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Transactions_G' then Value else null end ) Transactions_G,
           max(case when KPI='Transactions_G' then prevValue else null end ) Transactions_G_PrevX,
           max(case when KPI='Transactions_G' then prevyearvalue else null end ) Transactions_G_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'bounce_rate_G_per' KPI,
date, XTD,
ifnull(safe_divide(Bounces_G,Sessions_G),0) value,
ifnull(safe_divide(Bounces_G_prevX,Sessions_G_prevX),0) prevvalue,
ifnull(safe_divide(Bounces_G_PrevyearsameX,Sessions_G_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Bounces_G' then Value else null end ) Bounces_G,
           max(case when KPI='Bounces_G' then prevValue else null end ) Bounces_G_PrevX,
           max(case when KPI='Bounces_G' then prevyearvalue else null end ) Bounces_G_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Sessions_G' then Value else null end ) Sessions_G,
           max(case when KPI='Sessions_G' then prevValue else null end ) Sessions_G_PrevX,
           max(case when KPI='Sessions_G' then prevyearvalue else null end ) Sessions_G_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'dilution' KPI,
date, XTD,
ifnull(safe_divide((MSRP - NetRevenue),MSRP),0) value,
ifnull(safe_divide((MSRP_prevX - NetRevenue_prevX),MSRP_prevX),0) prevvalue,
ifnull(safe_divide((MSRP_PrevyearsameX - NetRevenue_PrevyearsameX),MSRP_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='MSRP' then Value else null end ) MSRP,
           max(case when KPI='MSRP' then prevValue else null end ) MSRP_PrevX,
           max(case when KPI='MSRP' then prevyearvalue else null end ) MSRP_PrevyearsameX
       
                ,
        
        
           max(case when KPI='NetRevenue' then Value else null end ) NetRevenue,
           max(case when KPI='NetRevenue' then prevValue else null end ) NetRevenue_PrevX,
           max(case when KPI='NetRevenue' then prevyearvalue else null end ) NetRevenue_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ATV' KPI,
date, XTD,
ifnull(safe_divide(NetRevenue,orders_auth),0) value,
ifnull(safe_divide(NetRevenue_prevX,orders_auth_prevX),0) prevvalue,
ifnull(safe_divide(NetRevenue_PrevyearsameX,orders_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='NetRevenue' then Value else null end ) NetRevenue,
           max(case when KPI='NetRevenue' then prevValue else null end ) NetRevenue_PrevX,
           max(case when KPI='NetRevenue' then prevyearvalue else null end ) NetRevenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='orders_auth' then Value else null end ) orders_auth,
           max(case when KPI='orders_auth' then prevValue else null end ) orders_auth_PrevX,
           max(case when KPI='orders_auth' then prevyearvalue else null end ) orders_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'UPT' KPI,
date, XTD,
ifnull(safe_divide(total_product_quantity_auth,orders_auth),0) value,
ifnull(safe_divide(total_product_quantity_auth_prevX,orders_auth_prevX),0) prevvalue,
ifnull(safe_divide(total_product_quantity_auth_PrevyearsameX,orders_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='total_product_quantity_auth' then Value else null end ) total_product_quantity_auth,
           max(case when KPI='total_product_quantity_auth' then prevValue else null end ) total_product_quantity_auth_PrevX,
           max(case when KPI='total_product_quantity_auth' then prevyearvalue else null end ) total_product_quantity_auth_PrevyearsameX
       
                ,
        
        
           max(case when KPI='orders_auth' then Value else null end ) orders_auth,
           max(case when KPI='orders_auth' then prevValue else null end ) orders_auth_PrevX,
           max(case when KPI='orders_auth' then prevyearvalue else null end ) orders_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'returns_volume_per' KPI,
date, XTD,
ifnull(safe_divide(ReturnedUnits,total_product_quantity_auth),0) value,
ifnull(safe_divide(ReturnedUnits_prevX,total_product_quantity_auth_prevX),0) prevvalue,
ifnull(safe_divide(ReturnedUnits_PrevyearsameX,total_product_quantity_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='ReturnedUnits' then Value else null end ) ReturnedUnits,
           max(case when KPI='ReturnedUnits' then prevValue else null end ) ReturnedUnits_PrevX,
           max(case when KPI='ReturnedUnits' then prevyearvalue else null end ) ReturnedUnits_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_product_quantity_auth' then Value else null end ) total_product_quantity_auth,
           max(case when KPI='total_product_quantity_auth' then prevValue else null end ) total_product_quantity_auth_PrevX,
           max(case when KPI='total_product_quantity_auth' then prevyearvalue else null end ) total_product_quantity_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'returns_value_per' KPI,
date, XTD,
ifnull(safe_divide(ReturnsRevenue,NetRevenue),0) value,
ifnull(safe_divide(ReturnsRevenue_prevX,NetRevenue_prevX),0) prevvalue,
ifnull(safe_divide(ReturnsRevenue_PrevyearsameX,NetRevenue_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='ReturnsRevenue' then Value else null end ) ReturnsRevenue,
           max(case when KPI='ReturnsRevenue' then prevValue else null end ) ReturnsRevenue_PrevX,
           max(case when KPI='ReturnsRevenue' then prevyearvalue else null end ) ReturnsRevenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='NetRevenue' then Value else null end ) NetRevenue,
           max(case when KPI='NetRevenue' then prevValue else null end ) NetRevenue_PrevX,
           max(case when KPI='NetRevenue' then prevyearvalue else null end ) NetRevenue_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'cancellation_per' KPI,
date, XTD,
ifnull(safe_divide(Itemscancelled,total_product_quantity),0) value,
ifnull(safe_divide(Itemscancelled_prevX,total_product_quantity_prevX),0) prevvalue,
ifnull(safe_divide(Itemscancelled_PrevyearsameX,total_product_quantity_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Itemscancelled' then Value else null end ) Itemscancelled,
           max(case when KPI='Itemscancelled' then prevValue else null end ) Itemscancelled_PrevX,
           max(case when KPI='Itemscancelled' then prevyearvalue else null end ) Itemscancelled_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_product_quantity' then Value else null end ) total_product_quantity,
           max(case when KPI='total_product_quantity' then prevValue else null end ) total_product_quantity_PrevX,
           max(case when KPI='total_product_quantity' then prevyearvalue else null end ) total_product_quantity_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'atv_first_purchase' KPI,
date, XTD,
ifnull(safe_divide(New_customer_Revenue,New_Customer_Orders),0) value,
ifnull(safe_divide(New_customer_Revenue_prevX,New_Customer_Orders_prevX),0) prevvalue,
ifnull(safe_divide(New_customer_Revenue_PrevyearsameX,New_Customer_Orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='New_customer_Revenue' then Value else null end ) New_customer_Revenue,
           max(case when KPI='New_customer_Revenue' then prevValue else null end ) New_customer_Revenue_PrevX,
           max(case when KPI='New_customer_Revenue' then prevyearvalue else null end ) New_customer_Revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='New_Customer_Orders' then Value else null end ) New_Customer_Orders,
           max(case when KPI='New_Customer_Orders' then prevValue else null end ) New_Customer_Orders_PrevX,
           max(case when KPI='New_Customer_Orders' then prevyearvalue else null end ) New_Customer_Orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'atv_repeat_purchase' KPI,
date, XTD,
ifnull(safe_divide(Existing_customer_Revenue,Existing_Customer_Orders),0) value,
ifnull(safe_divide(Existing_customer_Revenue_prevX,Existing_Customer_Orders_prevX),0) prevvalue,
ifnull(safe_divide(Existing_customer_Revenue_PrevyearsameX,Existing_Customer_Orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Existing_customer_Revenue' then Value else null end ) Existing_customer_Revenue,
           max(case when KPI='Existing_customer_Revenue' then prevValue else null end ) Existing_customer_Revenue_PrevX,
           max(case when KPI='Existing_customer_Revenue' then prevyearvalue else null end ) Existing_customer_Revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Existing_Customer_Orders' then Value else null end ) Existing_Customer_Orders,
           max(case when KPI='Existing_Customer_Orders' then prevValue else null end ) Existing_Customer_Orders_PrevX,
           max(case when KPI='Existing_Customer_Orders' then prevyearvalue else null end ) Existing_Customer_Orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Promotion_Dilution_Per' KPI,
date, XTD,
ifnull(1-safe_divide(total_item_price_auth,GrossRevenue),0) value,
ifnull(1-safe_divide(total_item_price_auth_prevX,GrossRevenue_prevX),0) prevvalue,
ifnull(1-safe_divide(total_item_price_auth_PrevyearsameX,GrossRevenue_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
                ,
        
        
           max(case when KPI='GrossRevenue' then Value else null end ) GrossRevenue,
           max(case when KPI='GrossRevenue' then prevValue else null end ) GrossRevenue_PrevX,
           max(case when KPI='GrossRevenue' then prevyearvalue else null end ) GrossRevenue_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Return_Dilution_Per' KPI,
date, XTD,
ifnull(safe_divide(ReturnsRevenue,GrossRevenue),0) value,
ifnull(safe_divide(ReturnsRevenue_prevX,GrossRevenue_prevX),0) prevvalue,
ifnull(safe_divide(ReturnsRevenue_PrevyearsameX,GrossRevenue_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='ReturnsRevenue' then Value else null end ) ReturnsRevenue,
           max(case when KPI='ReturnsRevenue' then prevValue else null end ) ReturnsRevenue_PrevX,
           max(case when KPI='ReturnsRevenue' then prevyearvalue else null end ) ReturnsRevenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='GrossRevenue' then Value else null end ) GrossRevenue,
           max(case when KPI='GrossRevenue' then prevValue else null end ) GrossRevenue_PrevX,
           max(case when KPI='GrossRevenue' then prevyearvalue else null end ) GrossRevenue_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'AUR' KPI,
date, XTD,
ifnull(safe_divide(total_item_price_auth,total_product_quantity_auth),0) value,
ifnull(safe_divide(total_item_price_auth_prevX,total_product_quantity_auth_prevX),0) prevvalue,
ifnull(safe_divide(total_item_price_auth_PrevyearsameX,total_product_quantity_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_product_quantity_auth' then Value else null end ) total_product_quantity_auth,
           max(case when KPI='total_product_quantity_auth' then prevValue else null end ) total_product_quantity_auth_PrevX,
           max(case when KPI='total_product_quantity_auth' then prevyearvalue else null end ) total_product_quantity_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'CTR_Per' KPI,
date, XTD,
ifnull(safe_divide(Clicks,Impressions),0) value,
ifnull(safe_divide(Clicks_prevX,Impressions_prevX),0) prevvalue,
ifnull(safe_divide(Clicks_PrevyearsameX,Impressions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Clicks' then Value else null end ) Clicks,
           max(case when KPI='Clicks' then prevValue else null end ) Clicks_PrevX,
           max(case when KPI='Clicks' then prevyearvalue else null end ) Clicks_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Impressions' then Value else null end ) Impressions,
           max(case when KPI='Impressions' then prevValue else null end ) Impressions_PrevX,
           max(case when KPI='Impressions' then prevyearvalue else null end ) Impressions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Cart_to_Detail_Per' KPI,
date, XTD,
ifnull(safe_divide(ProductAddstocart,ProductDetailViews),0) value,
ifnull(safe_divide(ProductAddstocart_prevX,ProductDetailViews_prevX),0) prevvalue,
ifnull(safe_divide(ProductAddstocart_PrevyearsameX,ProductDetailViews_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='ProductAddstocart' then Value else null end ) ProductAddstocart,
           max(case when KPI='ProductAddstocart' then prevValue else null end ) ProductAddstocart_PrevX,
           max(case when KPI='ProductAddstocart' then prevyearvalue else null end ) ProductAddstocart_PrevyearsameX
       
                ,
        
        
           max(case when KPI='ProductDetailViews' then Value else null end ) ProductDetailViews,
           max(case when KPI='ProductDetailViews' then prevValue else null end ) ProductDetailViews_PrevX,
           max(case when KPI='ProductDetailViews' then prevyearvalue else null end ) ProductDetailViews_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Buy_to_Detail_Per' KPI,
date, XTD,
ifnull(safe_divide(orders_auth,ProductDetailViews),0) value,
ifnull(safe_divide(orders_auth_prevX,ProductDetailViews_prevX),0) prevvalue,
ifnull(safe_divide(orders_auth_PrevyearsameX,ProductDetailViews_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='orders_auth' then Value else null end ) orders_auth,
           max(case when KPI='orders_auth' then prevValue else null end ) orders_auth_PrevX,
           max(case when KPI='orders_auth' then prevyearvalue else null end ) orders_auth_PrevyearsameX
       
                ,
        
        
           max(case when KPI='ProductDetailViews' then Value else null end ) ProductDetailViews,
           max(case when KPI='ProductDetailViews' then prevValue else null end ) ProductDetailViews_PrevX,
           max(case when KPI='ProductDetailViews' then prevyearvalue else null end ) ProductDetailViews_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Cart_to_Login' KPI,
date, XTD,
ifnull(safe_divide(LogginPage,AddstocartPage),0) value,
ifnull(safe_divide(LogginPage_prevX,AddstocartPage_prevX),0) prevvalue,
ifnull(safe_divide(LogginPage_PrevyearsameX,AddstocartPage_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='LogginPage' then Value else null end ) LogginPage,
           max(case when KPI='LogginPage' then prevValue else null end ) LogginPage_PrevX,
           max(case when KPI='LogginPage' then prevyearvalue else null end ) LogginPage_PrevyearsameX
       
                ,
        
        
           max(case when KPI='AddstocartPage' then Value else null end ) AddstocartPage,
           max(case when KPI='AddstocartPage' then prevValue else null end ) AddstocartPage_PrevX,
           max(case when KPI='AddstocartPage' then prevyearvalue else null end ) AddstocartPage_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Login_to_Shipping' KPI,
date, XTD,
ifnull(safe_divide(ShippingPage,LogginPage),0) value,
ifnull(safe_divide(ShippingPage_prevX,LogginPage_prevX),0) prevvalue,
ifnull(safe_divide(ShippingPage_PrevyearsameX,LogginPage_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='ShippingPage' then Value else null end ) ShippingPage,
           max(case when KPI='ShippingPage' then prevValue else null end ) ShippingPage_PrevX,
           max(case when KPI='ShippingPage' then prevyearvalue else null end ) ShippingPage_PrevyearsameX
       
                ,
        
        
           max(case when KPI='LogginPage' then Value else null end ) LogginPage,
           max(case when KPI='LogginPage' then prevValue else null end ) LogginPage_PrevX,
           max(case when KPI='LogginPage' then prevyearvalue else null end ) LogginPage_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'shipping_to_payment' KPI,
date, XTD,
ifnull(safe_divide(PaymentPage,ShippingPage),0) value,
ifnull(safe_divide(PaymentPage_prevX,ShippingPage_prevX),0) prevvalue,
ifnull(safe_divide(PaymentPage_PrevyearsameX,ShippingPage_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='PaymentPage' then Value else null end ) PaymentPage,
           max(case when KPI='PaymentPage' then prevValue else null end ) PaymentPage_PrevX,
           max(case when KPI='PaymentPage' then prevyearvalue else null end ) PaymentPage_PrevyearsameX
       
                ,
        
        
           max(case when KPI='ShippingPage' then Value else null end ) ShippingPage,
           max(case when KPI='ShippingPage' then prevValue else null end ) ShippingPage_PrevX,
           max(case when KPI='ShippingPage' then prevyearvalue else null end ) ShippingPage_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Payment_to_transcation' KPI,
date, XTD,
ifnull(safe_divide(Transactions,PaymentPage),0) value,
ifnull(safe_divide(Transactions_prevX,PaymentPage_prevX),0) prevvalue,
ifnull(safe_divide(Transactions_PrevyearsameX,PaymentPage_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Transactions' then Value else null end ) Transactions,
           max(case when KPI='Transactions' then prevValue else null end ) Transactions_PrevX,
           max(case when KPI='Transactions' then prevyearvalue else null end ) Transactions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='PaymentPage' then Value else null end ) PaymentPage,
           max(case when KPI='PaymentPage' then prevValue else null end ) PaymentPage_PrevX,
           max(case when KPI='PaymentPage' then prevyearvalue else null end ) PaymentPage_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ExitRate' KPI,
date, XTD,
ifnull(safe_divide(exits,sessions),0) value,
ifnull(safe_divide(exits_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(exits_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='exits' then Value else null end ) exits,
           max(case when KPI='exits' then prevValue else null end ) exits_PrevX,
           max(case when KPI='exits' then prevyearvalue else null end ) exits_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'sessions_to_Productviews' KPI,
date, XTD,
ifnull(safe_divide(Productviews,sessions),0) value,
ifnull(safe_divide(Productviews_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(Productviews_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Productviews' then Value else null end ) Productviews,
           max(case when KPI='Productviews' then prevValue else null end ) Productviews_PrevX,
           max(case when KPI='Productviews' then prevyearvalue else null end ) Productviews_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'LP_bounceRate' KPI,
date, XTD,
ifnull(safe_divide(LP_Lan_Bounce,LP_Lan_Sessions),0) value,
ifnull(safe_divide(LP_Lan_Bounce_prevX,LP_Lan_Sessions_prevX),0) prevvalue,
ifnull(safe_divide(LP_Lan_Bounce_PrevyearsameX,LP_Lan_Sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='LP_Lan_Bounce' then Value else null end ) LP_Lan_Bounce,
           max(case when KPI='LP_Lan_Bounce' then prevValue else null end ) LP_Lan_Bounce_PrevX,
           max(case when KPI='LP_Lan_Bounce' then prevyearvalue else null end ) LP_Lan_Bounce_PrevyearsameX
       
                ,
        
        
           max(case when KPI='LP_Lan_Sessions' then Value else null end ) LP_Lan_Sessions,
           max(case when KPI='LP_Lan_Sessions' then prevValue else null end ) LP_Lan_Sessions_PrevX,
           max(case when KPI='LP_Lan_Sessions' then prevyearvalue else null end ) LP_Lan_Sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'LP_ConvRate' KPI,
date, XTD,
ifnull(safe_divide(LP_Lan_Transactions,LP_Lan_sessions),0) value,
ifnull(safe_divide(LP_Lan_Transactions_prevX,LP_Lan_Sessions_prevX),0) prevvalue,
ifnull(safe_divide(LP_Lan_Transactions_PrevyearsameX,LP_Lan_Sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='LP_Lan_Transactions' then Value else null end ) LP_Lan_Transactions,
           max(case when KPI='LP_Lan_Transactions' then prevValue else null end ) LP_Lan_Transactions_PrevX,
           max(case when KPI='LP_Lan_Transactions' then prevyearvalue else null end ) LP_Lan_Transactions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='LP_Lan_Sessions' then Value else null end ) LP_Lan_Sessions,
           max(case when KPI='LP_Lan_Sessions' then prevValue else null end ) LP_Lan_Sessions_PrevX,
           max(case when KPI='LP_Lan_Sessions' then prevyearvalue else null end ) LP_Lan_Sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'H_bounceRate' KPI,
date, XTD,
ifnull(safe_divide(H_Lan_Bounces,H_Lan_Sessions),0) value,
ifnull(safe_divide(H_Lan_Bounces_prevX,H_Lan_Sessions_prevX),0) prevvalue,
ifnull(safe_divide(H_Lan_Bounces_PrevyearsameX,H_Lan_Sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='H_Lan_Bounces' then Value else null end ) H_Lan_Bounces,
           max(case when KPI='H_Lan_Bounces' then prevValue else null end ) H_Lan_Bounces_PrevX,
           max(case when KPI='H_Lan_Bounces' then prevyearvalue else null end ) H_Lan_Bounces_PrevyearsameX
       
                ,
        
        
           max(case when KPI='H_Lan_Sessions' then Value else null end ) H_Lan_Sessions,
           max(case when KPI='H_Lan_Sessions' then prevValue else null end ) H_Lan_Sessions_PrevX,
           max(case when KPI='H_Lan_Sessions' then prevyearvalue else null end ) H_Lan_Sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'H_ConvRate' KPI,
date, XTD,
ifnull(safe_divide(H_Lan_transactions,H_Lan_Sessions),0) value,
ifnull(safe_divide(H_Lan_transactions_prevX,H_Lan_Sessions_prevX),0) prevvalue,
ifnull(safe_divide(H_Lan_transactions_PrevyearsameX,H_Lan_Sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='H_Lan_Sessions' then Value else null end ) H_Lan_Sessions,
           max(case when KPI='H_Lan_Sessions' then prevValue else null end ) H_Lan_Sessions_PrevX,
           max(case when KPI='H_Lan_Sessions' then prevyearvalue else null end ) H_Lan_Sessions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='H_Lan_transactions' then Value else null end ) H_Lan_transactions,
           max(case when KPI='H_Lan_transactions' then prevValue else null end ) H_Lan_transactions_PrevX,
           max(case when KPI='H_Lan_transactions' then prevyearvalue else null end ) H_Lan_transactions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'P_bounceRate' KPI,
date, XTD,
ifnull(safe_divide(P_bounces,P_Sessions),0) value,
ifnull(safe_divide(P_bounces_prevX,P_Sessions_prevX),0) prevvalue,
ifnull(safe_divide(P_bounces_PrevyearsameX,P_Sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='P_bounces' then Value else null end ) P_bounces,
           max(case when KPI='P_bounces' then prevValue else null end ) P_bounces_PrevX,
           max(case when KPI='P_bounces' then prevyearvalue else null end ) P_bounces_PrevyearsameX
       
                ,
        
        
           max(case when KPI='P_Sessions' then Value else null end ) P_Sessions,
           max(case when KPI='P_Sessions' then prevValue else null end ) P_Sessions_PrevX,
           max(case when KPI='P_Sessions' then prevyearvalue else null end ) P_Sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'P_ConvRate' KPI,
date, XTD,
ifnull(safe_divide(P_Transactions,P_Sessions),0) value,
ifnull(safe_divide(P_Transactions_prevX,P_Sessions_prevX),0) prevvalue,
ifnull(safe_divide(P_Transactions_PrevyearsameX,P_Sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='P_Transactions' then Value else null end ) P_Transactions,
           max(case when KPI='P_Transactions' then prevValue else null end ) P_Transactions_PrevX,
           max(case when KPI='P_Transactions' then prevyearvalue else null end ) P_Transactions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='P_Sessions' then Value else null end ) P_Sessions,
           max(case when KPI='P_Sessions' then prevValue else null end ) P_Sessions_PrevX,
           max(case when KPI='P_Sessions' then prevyearvalue else null end ) P_Sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'H_bounce_rate' KPI,
date, XTD,
ifnull(safe_divide(H_Lan_Bounces,H_Lan_Sessions),0) value,
ifnull(safe_divide(H_Lan_Bounces_prevX,H_Lan_sessions_prevX),0) prevvalue,
ifnull(safe_divide(H_Lan_Bounces_PrevyearsameX,H_Lan_Sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='H_Lan_Bounces' then Value else null end ) H_Lan_Bounces,
           max(case when KPI='H_Lan_Bounces' then prevValue else null end ) H_Lan_Bounces_PrevX,
           max(case when KPI='H_Lan_Bounces' then prevyearvalue else null end ) H_Lan_Bounces_PrevyearsameX
       
                ,
        
        
           max(case when KPI='H_Lan_Sessions' then Value else null end ) H_Lan_Sessions,
           max(case when KPI='H_Lan_Sessions' then prevValue else null end ) H_Lan_Sessions_PrevX,
           max(case when KPI='H_Lan_Sessions' then prevyearvalue else null end ) H_Lan_Sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'H_Non_Bounce_Sessions' KPI,
date, XTD,
ifnull(H_Sessions-H_Bounces,0) value,
ifnull(H_Sessions_prevX-H_Bounces_prevX,0) prevvalue,
ifnull(H_Sessions_PrevyearsameX-H_Bounces_PrevyearsameX,0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='H_Sessions' then Value else null end ) H_Sessions,
           max(case when KPI='H_Sessions' then prevValue else null end ) H_Sessions_PrevX,
           max(case when KPI='H_Sessions' then prevyearvalue else null end ) H_Sessions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='H_Bounces' then Value else null end ) H_Bounces,
           max(case when KPI='H_Bounces' then prevValue else null end ) H_Bounces_PrevX,
           max(case when KPI='H_Bounces' then prevyearvalue else null end ) H_Bounces_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'H_Conv_Rate' KPI,
date, XTD,
ifnull(safe_divide(H_Lan_transactions,H_Lan_Sessions),0) value,
ifnull(safe_divide(H_Lan_transactions_prevX,H_Lan_Sessions_prevX),0) prevvalue,
ifnull(safe_divide(H_Lan_transactions_PrevyearsameX,H_Lan_Sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='H_Lan_Sessions' then Value else null end ) H_Lan_Sessions,
           max(case when KPI='H_Lan_Sessions' then prevValue else null end ) H_Lan_Sessions_PrevX,
           max(case when KPI='H_Lan_Sessions' then prevyearvalue else null end ) H_Lan_Sessions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='H_Lan_transactions' then Value else null end ) H_Lan_transactions,
           max(case when KPI='H_Lan_transactions' then prevValue else null end ) H_Lan_transactions_PrevX,
           max(case when KPI='H_Lan_transactions' then prevyearvalue else null end ) H_Lan_transactions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'LP_PvtoCart' KPI,
date, XTD,
ifnull(safe_divide(LP_addstocart,LP_productDetailViews),0) value,
ifnull(safe_divide(LP_addstocart_prevX,LP_productDetailViews_prevX),0) prevvalue,
ifnull(safe_divide(LP_addstocart_PrevyearsameX,LP_productDetailViews_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='LP_addstocart' then Value else null end ) LP_addstocart,
           max(case when KPI='LP_addstocart' then prevValue else null end ) LP_addstocart_PrevX,
           max(case when KPI='LP_addstocart' then prevyearvalue else null end ) LP_addstocart_PrevyearsameX
       
                ,
        
        
           max(case when KPI='LP_productDetailViews' then Value else null end ) LP_productDetailViews,
           max(case when KPI='LP_productDetailViews' then prevValue else null end ) LP_productDetailViews_PrevX,
           max(case when KPI='LP_productDetailViews' then prevyearvalue else null end ) LP_productDetailViews_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Checkout_Conversion_Rate' KPI,
date, XTD,
ifnull(safe_divide(Transactions,Checkout),0) value,
ifnull(safe_divide(Transactions_prevX,Checkout_prevX),0) prevvalue,
ifnull(safe_divide(Transactions_PrevyearsameX,Checkout_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Transactions' then Value else null end ) Transactions,
           max(case when KPI='Transactions' then prevValue else null end ) Transactions_PrevX,
           max(case when KPI='Transactions' then prevyearvalue else null end ) Transactions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Checkout' then Value else null end ) Checkout,
           max(case when KPI='Checkout' then prevValue else null end ) Checkout_PrevX,
           max(case when KPI='Checkout' then prevyearvalue else null end ) Checkout_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Total_Sales_Value_Without_VAT' KPI,
date, XTD,
ifnull(total_item_price_auth-tax_amount,0) value,
ifnull(total_item_price_auth_prevX-tax_amount_prevX,0) prevvalue,
ifnull(total_item_price_auth_PrevyearsameX-tax_amount_PrevyearsameX,0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
                ,
        
        
           max(case when KPI='tax_amount' then Value else null end ) tax_amount,
           max(case when KPI='tax_amount' then prevValue else null end ) tax_amount_PrevX,
           max(case when KPI='tax_amount' then prevyearvalue else null end ) tax_amount_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'cancellation_order_per' KPI,
date, XTD,
ifnull(safe_divide(cancelled_orders,orders),0) value,
ifnull(safe_divide(cancelled_orders_prevX,orders_prevX),0) prevvalue,
ifnull(safe_divide(cancelled_orders_PrevyearsameX,orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='cancelled_orders' then Value else null end ) cancelled_orders,
           max(case when KPI='cancelled_orders' then prevValue else null end ) cancelled_orders_PrevX,
           max(case when KPI='cancelled_orders' then prevyearvalue else null end ) cancelled_orders_PrevyearsameX
       
                ,
        
        
           max(case when KPI='orders' then Value else null end ) orders,
           max(case when KPI='orders' then prevValue else null end ) orders_PrevX,
           max(case when KPI='orders' then prevyearvalue else null end ) orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'cod_per' KPI,
date, XTD,
ifnull(safe_divide(cod,orders_auth),0) value,
ifnull(safe_divide(cod_prevX,orders_auth_prevX),0) prevvalue,
ifnull(safe_divide(cod_PrevyearsameX,orders_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='cod' then Value else null end ) cod,
           max(case when KPI='cod' then prevValue else null end ) cod_PrevX,
           max(case when KPI='cod' then prevyearvalue else null end ) cod_PrevyearsameX
       
                ,
        
        
           max(case when KPI='orders_auth' then Value else null end ) orders_auth,
           max(case when KPI='orders_auth' then prevValue else null end ) orders_auth_PrevX,
           max(case when KPI='orders_auth' then prevyearvalue else null end ) orders_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'cc_per' KPI,
date, XTD,
ifnull(safe_divide(cc,orders_auth),0) value,
ifnull(safe_divide(cc_prevX,orders_auth_prevX),0) prevvalue,
ifnull(safe_divide(cc_PrevyearsameX,orders_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='cc' then Value else null end ) cc,
           max(case when KPI='cc' then prevValue else null end ) cc_PrevX,
           max(case when KPI='cc' then prevyearvalue else null end ) cc_PrevyearsameX
       
                ,
        
        
           max(case when KPI='orders_auth' then Value else null end ) orders_auth,
           max(case when KPI='orders_auth' then prevValue else null end ) orders_auth_PrevX,
           max(case when KPI='orders_auth' then prevyearvalue else null end ) orders_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'tabby_per' KPI,
date, XTD,
ifnull(safe_divide(tabby,orders_auth),0) value,
ifnull(safe_divide(tabby_prevX,orders_auth_prevX),0) prevvalue,
ifnull(safe_divide(tabby_PrevyearsameX,orders_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='tabby' then Value else null end ) tabby,
           max(case when KPI='tabby' then prevValue else null end ) tabby_PrevX,
           max(case when KPI='tabby' then prevyearvalue else null end ) tabby_PrevyearsameX
       
                ,
        
        
           max(case when KPI='orders_auth' then Value else null end ) orders_auth,
           max(case when KPI='orders_auth' then prevValue else null end ) orders_auth_PrevX,
           max(case when KPI='orders_auth' then prevyearvalue else null end ) orders_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'returned_orders_per' KPI,
date, XTD,
ifnull(safe_divide(returned_orders,orders),0) value,
ifnull(safe_divide(returned_orders_prevX,orders_prevX),0) prevvalue,
ifnull(safe_divide(returned_orders_PrevyearsameX,orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='returned_orders' then Value else null end ) returned_orders,
           max(case when KPI='returned_orders' then prevValue else null end ) returned_orders_PrevX,
           max(case when KPI='returned_orders' then prevyearvalue else null end ) returned_orders_PrevyearsameX
       
                ,
        
        
           max(case when KPI='orders' then Value else null end ) orders,
           max(case when KPI='orders' then prevValue else null end ) orders_PrevX,
           max(case when KPI='orders' then prevyearvalue else null end ) orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'sessions_per_visitors' KPI,
date, XTD,
ifnull(safe_divide(sessions,day_level_users),0) value,
ifnull(safe_divide(sessions_prevX,day_level_users_prevX),0) prevvalue,
ifnull(safe_divide(sessions_PrevyearsameX,day_level_users_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='day_level_users' then Value else null end ) day_level_users,
           max(case when KPI='day_level_users' then prevValue else null end ) day_level_users_PrevX,
           max(case when KPI='day_level_users' then prevyearvalue else null end ) day_level_users_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'pages_per_sessions' KPI,
date, XTD,
ifnull(safe_divide(pageviews,sessions),0) value,
ifnull(safe_divide(pageviews_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(pageviews_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='pageviews' then Value else null end ) pageviews,
           max(case when KPI='pageviews' then prevValue else null end ) pageviews_PrevX,
           max(case when KPI='pageviews' then prevyearvalue else null end ) pageviews_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'mobile_per' KPI,
date, XTD,
ifnull(safe_divide(mobile,sessions),0) value,
ifnull(safe_divide(mobile_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(mobile_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='mobile' then Value else null end ) mobile,
           max(case when KPI='mobile' then prevValue else null end ) mobile_PrevX,
           max(case when KPI='mobile' then prevyearvalue else null end ) mobile_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'tablet_per' KPI,
date, XTD,
ifnull(safe_divide(tablet,sessions),0) value,
ifnull(safe_divide(tablet_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(tablet_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='tablet' then Value else null end ) tablet,
           max(case when KPI='tablet' then prevValue else null end ) tablet_PrevX,
           max(case when KPI='tablet' then prevyearvalue else null end ) tablet_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'desktop_per' KPI,
date, XTD,
ifnull(safe_divide(desktop,sessions),0) value,
ifnull(safe_divide(desktop_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(desktop_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='desktop' then Value else null end ) desktop,
           max(case when KPI='desktop' then prevValue else null end ) desktop_PrevX,
           max(case when KPI='desktop' then prevyearvalue else null end ) desktop_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'age_18_24_per' KPI,
date, XTD,
ifnull(safe_divide(newusers_with_age_18_24,age_new_users),0) value,
ifnull(safe_divide(newusers_with_age_18_24_prevX,age_new_users_prevX),0) prevvalue,
ifnull(safe_divide(newusers_with_age_18_24_PrevyearsameX,age_new_users_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='newusers_with_age_18_24' then Value else null end ) newusers_with_age_18_24,
           max(case when KPI='newusers_with_age_18_24' then prevValue else null end ) newusers_with_age_18_24_PrevX,
           max(case when KPI='newusers_with_age_18_24' then prevyearvalue else null end ) newusers_with_age_18_24_PrevyearsameX
       
                ,
        
        
           max(case when KPI='age_new_users' then Value else null end ) age_new_users,
           max(case when KPI='age_new_users' then prevValue else null end ) age_new_users_PrevX,
           max(case when KPI='age_new_users' then prevyearvalue else null end ) age_new_users_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'age_25_34_per' KPI,
date, XTD,
ifnull(safe_divide(newusers_with_age_25_34,age_new_users),0) value,
ifnull(safe_divide(newusers_with_age_25_34_prevX,age_new_users_prevX),0) prevvalue,
ifnull(safe_divide(newusers_with_age_25_34_PrevyearsameX,age_new_users_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='newusers_with_age_25_34' then Value else null end ) newusers_with_age_25_34,
           max(case when KPI='newusers_with_age_25_34' then prevValue else null end ) newusers_with_age_25_34_PrevX,
           max(case when KPI='newusers_with_age_25_34' then prevyearvalue else null end ) newusers_with_age_25_34_PrevyearsameX
       
                ,
        
        
           max(case when KPI='age_new_users' then Value else null end ) age_new_users,
           max(case when KPI='age_new_users' then prevValue else null end ) age_new_users_PrevX,
           max(case when KPI='age_new_users' then prevyearvalue else null end ) age_new_users_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'age_35_44_per' KPI,
date, XTD,
ifnull(safe_divide(newusers_with_age_35_44,age_new_users),0) value,
ifnull(safe_divide(newusers_with_age_35_44_prevX,age_new_users_prevX),0) prevvalue,
ifnull(safe_divide(newusers_with_age_35_44_PrevyearsameX,age_new_users_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='newusers_with_age_35_44' then Value else null end ) newusers_with_age_35_44,
           max(case when KPI='newusers_with_age_35_44' then prevValue else null end ) newusers_with_age_35_44_PrevX,
           max(case when KPI='newusers_with_age_35_44' then prevyearvalue else null end ) newusers_with_age_35_44_PrevyearsameX
       
                ,
        
        
           max(case when KPI='age_new_users' then Value else null end ) age_new_users,
           max(case when KPI='age_new_users' then prevValue else null end ) age_new_users_PrevX,
           max(case when KPI='age_new_users' then prevyearvalue else null end ) age_new_users_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'age_above_45_per' KPI,
date, XTD,
ifnull(safe_divide(newusers_with_age_above_45,age_new_users),0) value,
ifnull(safe_divide(newusers_with_age_above_45_prevX,age_new_users_prevX),0) prevvalue,
ifnull(safe_divide(newusers_with_age_above_45_PrevyearsameX,age_new_users_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='newusers_with_age_above_45' then Value else null end ) newusers_with_age_above_45,
           max(case when KPI='newusers_with_age_above_45' then prevValue else null end ) newusers_with_age_above_45_PrevX,
           max(case when KPI='newusers_with_age_above_45' then prevyearvalue else null end ) newusers_with_age_above_45_PrevyearsameX
       
                ,
        
        
           max(case when KPI='age_new_users' then Value else null end ) age_new_users,
           max(case when KPI='age_new_users' then prevValue else null end ) age_new_users_PrevX,
           max(case when KPI='age_new_users' then prevyearvalue else null end ) age_new_users_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'gender_male_newusers_per' KPI,
date, XTD,
ifnull(safe_divide(newusers_with_gender_male,gender_new_users),0) value,
ifnull(safe_divide(newusers_with_gender_male_prevX,gender_new_users_prevX),0) prevvalue,
ifnull(safe_divide(newusers_with_gender_male_PrevyearsameX,gender_new_users_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='newusers_with_gender_male' then Value else null end ) newusers_with_gender_male,
           max(case when KPI='newusers_with_gender_male' then prevValue else null end ) newusers_with_gender_male_PrevX,
           max(case when KPI='newusers_with_gender_male' then prevyearvalue else null end ) newusers_with_gender_male_PrevyearsameX
       
                ,
        
        
           max(case when KPI='gender_new_users' then Value else null end ) gender_new_users,
           max(case when KPI='gender_new_users' then prevValue else null end ) gender_new_users_PrevX,
           max(case when KPI='gender_new_users' then prevyearvalue else null end ) gender_new_users_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'gender_female_newusers_per' KPI,
date, XTD,
ifnull(safe_divide(newusers_with_gender_female,gender_new_users),0) value,
ifnull(safe_divide(newusers_with_gender_female_prevX,gender_new_users_prevX),0) prevvalue,
ifnull(safe_divide(newusers_with_gender_female_PrevyearsameX,gender_new_users_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='newusers_with_gender_female' then Value else null end ) newusers_with_gender_female,
           max(case when KPI='newusers_with_gender_female' then prevValue else null end ) newusers_with_gender_female_PrevX,
           max(case when KPI='newusers_with_gender_female' then prevyearvalue else null end ) newusers_with_gender_female_PrevyearsameX
       
                ,
        
        
           max(case when KPI='gender_new_users' then Value else null end ) gender_new_users,
           max(case when KPI='gender_new_users' then prevValue else null end ) gender_new_users_PrevX,
           max(case when KPI='gender_new_users' then prevyearvalue else null end ) gender_new_users_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'new_visitor_sessions_per' KPI,
date, XTD,
ifnull(safe_divide(new_visitor_sessions,sessions),0) value,
ifnull(safe_divide(new_visitor_sessions_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(new_visitor_sessions_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='new_visitor_sessions' then Value else null end ) new_visitor_sessions,
           max(case when KPI='new_visitor_sessions' then prevValue else null end ) new_visitor_sessions_PrevX,
           max(case when KPI='new_visitor_sessions' then prevyearvalue else null end ) new_visitor_sessions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'returning_visitor_sessions_per' KPI,
date, XTD,
ifnull(safe_divide(returning_visitor_sessions,sessions),0) value,
ifnull(safe_divide(returning_visitor_sessions_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(returning_visitor_sessions_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='returning_visitor_sessions' then Value else null end ) returning_visitor_sessions,
           max(case when KPI='returning_visitor_sessions' then prevValue else null end ) returning_visitor_sessions_PrevX,
           max(case when KPI='returning_visitor_sessions' then prevyearvalue else null end ) returning_visitor_sessions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Snapchat_paid_sessions_per' KPI,
date, XTD,
ifnull(safe_divide(Snapchat_paid_sessions,sessions),0) value,
ifnull(safe_divide(Snapchat_paid_sessions_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(Snapchat_paid_sessions_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Snapchat_paid_sessions' then Value else null end ) Snapchat_paid_sessions,
           max(case when KPI='Snapchat_paid_sessions' then prevValue else null end ) Snapchat_paid_sessions_PrevX,
           max(case when KPI='Snapchat_paid_sessions' then prevyearvalue else null end ) Snapchat_paid_sessions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Snapchat_unpaid_sessions_per' KPI,
date, XTD,
ifnull(safe_divide(Snapchat_unpaid_sessions,sessions),0) value,
ifnull(safe_divide(Snapchat_unpaid_sessions_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(Snapchat_unpaid_sessions_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Snapchat_unpaid_sessions' then Value else null end ) Snapchat_unpaid_sessions,
           max(case when KPI='Snapchat_unpaid_sessions' then prevValue else null end ) Snapchat_unpaid_sessions_PrevX,
           max(case when KPI='Snapchat_unpaid_sessions' then prevyearvalue else null end ) Snapchat_unpaid_sessions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Facebook_Inhouse_Paid_sessions_per' KPI,
date, XTD,
ifnull(safe_divide(Facebook_Inhouse_Paid_sessions,sessions),0) value,
ifnull(safe_divide(Facebook_Inhouse_Paid_sessions_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(Facebook_Inhouse_Paid_sessions_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Facebook_Inhouse_Paid_sessions' then Value else null end ) Facebook_Inhouse_Paid_sessions,
           max(case when KPI='Facebook_Inhouse_Paid_sessions' then prevValue else null end ) Facebook_Inhouse_Paid_sessions_PrevX,
           max(case when KPI='Facebook_Inhouse_Paid_sessions' then prevyearvalue else null end ) Facebook_Inhouse_Paid_sessions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Facebook_Dynamic_Remarketing_sessions_per' KPI,
date, XTD,
ifnull(safe_divide(Facebook_Dynamic_Remarketing_sessions,sessions),0) value,
ifnull(safe_divide(Facebook_Dynamic_Remarketing_sessions_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(Facebook_Dynamic_Remarketing_sessions_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Facebook_Dynamic_Remarketing_sessions' then Value else null end ) Facebook_Dynamic_Remarketing_sessions,
           max(case when KPI='Facebook_Dynamic_Remarketing_sessions' then prevValue else null end ) Facebook_Dynamic_Remarketing_sessions_PrevX,
           max(case when KPI='Facebook_Dynamic_Remarketing_sessions' then prevyearvalue else null end ) Facebook_Dynamic_Remarketing_sessions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Facebook_Unpaid_sessions_per' KPI,
date, XTD,
ifnull(safe_divide(Facebook_Unpaid_sessions,sessions),0) value,
ifnull(safe_divide(Facebook_Unpaid_sessions_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(Facebook_Unpaid_sessions_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Facebook_Unpaid_sessions' then Value else null end ) Facebook_Unpaid_sessions,
           max(case when KPI='Facebook_Unpaid_sessions' then prevValue else null end ) Facebook_Unpaid_sessions_PrevX,
           max(case when KPI='Facebook_Unpaid_sessions' then prevyearvalue else null end ) Facebook_Unpaid_sessions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Instagram_paid_sessions_per' KPI,
date, XTD,
ifnull(safe_divide(Instagram_paid_sessions,sessions),0) value,
ifnull(safe_divide(Instagram_paid_sessions_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(Instagram_paid_sessions_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Instagram_paid_sessions' then Value else null end ) Instagram_paid_sessions,
           max(case when KPI='Instagram_paid_sessions' then prevValue else null end ) Instagram_paid_sessions_PrevX,
           max(case when KPI='Instagram_paid_sessions' then prevyearvalue else null end ) Instagram_paid_sessions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Others_Sessions_per' KPI,
date, XTD,
ifnull(safe_divide(Others_Sessions,sessions),0) value,
ifnull(safe_divide(Others_Sessions_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(Others_Sessions_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Others_Sessions' then Value else null end ) Others_Sessions,
           max(case when KPI='Others_Sessions' then prevValue else null end ) Others_Sessions_PrevX,
           max(case when KPI='Others_Sessions' then prevyearvalue else null end ) Others_Sessions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Influencers_codes_per' KPI,
date, XTD,
ifnull(safe_divide(influencersrevenue,total_item_price_auth),0) value,
ifnull(safe_divide(influencersrevenue_prevX,total_item_price_auth_prevX),0) prevvalue,
ifnull(safe_divide(influencersrevenue_PrevyearsameX,total_item_price_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='influencersrevenue' then Value else null end ) influencersrevenue,
           max(case when KPI='influencersrevenue' then prevValue else null end ) influencersrevenue_PrevX,
           max(case when KPI='influencersrevenue' then prevyearvalue else null end ) influencersrevenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Unique_Visitors' KPI,
date, XTD,
ifnull(visitors-new_visitor_users,0) value,
ifnull(visitors_prevX-new_visitor_users_prevX,0) prevvalue,
ifnull(visitors_PrevyearsameX-new_visitor_users_PrevyearsameX,0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='visitors' then Value else null end ) visitors,
           max(case when KPI='visitors' then prevValue else null end ) visitors_PrevX,
           max(case when KPI='visitors' then prevyearvalue else null end ) visitors_PrevyearsameX
       
                ,
        
        
           max(case when KPI='new_visitor_users' then Value else null end ) new_visitor_users,
           max(case when KPI='new_visitor_users' then prevValue else null end ) new_visitor_users_PrevX,
           max(case when KPI='new_visitor_users' then prevyearvalue else null end ) new_visitor_users_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'avgSessionDuration1' KPI,
date, XTD,
ifnull(safe_divide(SessionDuration,sessions),0) value,
ifnull(safe_divide(SessionDuration_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(SessionDuration_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='SessionDuration' then Value else null end ) SessionDuration,
           max(case when KPI='SessionDuration' then prevValue else null end ) SessionDuration_PrevX,
           max(case when KPI='SessionDuration' then prevyearvalue else null end ) SessionDuration_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'avgdailysales' KPI,
date, XTD,
ifnull(safe_divide(total_item_price_auth,daylevel),0) value,
ifnull(safe_divide(total_item_price_auth_prevX,daylevel_prevX),0) prevvalue,
ifnull(safe_divide(total_item_price_auth_PrevyearsameX,daylevel_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
                ,
        
        
           max(case when KPI='daylevel' then Value else null end ) daylevel,
           max(case when KPI='daylevel' then prevValue else null end ) daylevel_PrevX,
           max(case when KPI='daylevel' then prevyearvalue else null end ) daylevel_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'avgdailyunits' KPI,
date, XTD,
ifnull(safe_divide(total_product_quantity_auth,daylevel),0) value,
ifnull(safe_divide(total_product_quantity_auth_prevX,daylevel_prevX),0) prevvalue,
ifnull(safe_divide(total_product_quantity_auth_PrevyearsameX,daylevel_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='total_product_quantity_auth' then Value else null end ) total_product_quantity_auth,
           max(case when KPI='total_product_quantity_auth' then prevValue else null end ) total_product_quantity_auth_PrevX,
           max(case when KPI='total_product_quantity_auth' then prevyearvalue else null end ) total_product_quantity_auth_PrevyearsameX
       
                ,
        
        
           max(case when KPI='daylevel' then Value else null end ) daylevel,
           max(case when KPI='daylevel' then prevValue else null end ) daylevel_PrevX,
           max(case when KPI='daylevel' then prevyearvalue else null end ) daylevel_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'influencersorders_per' KPI,
date, XTD,
ifnull(safe_divide(influencersorders,orders_auth),0) value,
ifnull(safe_divide(influencersorders_prevX,orders_auth_prevX),0) prevvalue,
ifnull(safe_divide(influencersorders_PrevyearsameX,orders_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='influencersorders' then Value else null end ) influencersorders,
           max(case when KPI='influencersorders' then prevValue else null end ) influencersorders_PrevX,
           max(case when KPI='influencersorders' then prevyearvalue else null end ) influencersorders_PrevyearsameX
       
                ,
        
        
           max(case when KPI='orders_auth' then Value else null end ) orders_auth,
           max(case when KPI='orders_auth' then prevValue else null end ) orders_auth_PrevX,
           max(case when KPI='orders_auth' then prevyearvalue else null end ) orders_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'age_18_24_sessions_per' KPI,
date, XTD,
ifnull(safe_divide(sessions_with_age_18_24,age_sessions),0) value,
ifnull(safe_divide(sessions_with_age_18_24_prevX,age_sessions_prevX),0) prevvalue,
ifnull(safe_divide(sessions_with_age_18_24_PrevyearsameX,age_sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='sessions_with_age_18_24' then Value else null end ) sessions_with_age_18_24,
           max(case when KPI='sessions_with_age_18_24' then prevValue else null end ) sessions_with_age_18_24_PrevX,
           max(case when KPI='sessions_with_age_18_24' then prevyearvalue else null end ) sessions_with_age_18_24_PrevyearsameX
       
                ,
        
        
           max(case when KPI='age_sessions' then Value else null end ) age_sessions,
           max(case when KPI='age_sessions' then prevValue else null end ) age_sessions_PrevX,
           max(case when KPI='age_sessions' then prevyearvalue else null end ) age_sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'age_25_34_sessions_per' KPI,
date, XTD,
ifnull(safe_divide(sessions_with_age_25_34,age_sessions),0) value,
ifnull(safe_divide(sessions_with_age_25_34_prevX,age_sessions_prevX),0) prevvalue,
ifnull(safe_divide(sessions_with_age_25_34_PrevyearsameX,age_sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='sessions_with_age_25_34' then Value else null end ) sessions_with_age_25_34,
           max(case when KPI='sessions_with_age_25_34' then prevValue else null end ) sessions_with_age_25_34_PrevX,
           max(case when KPI='sessions_with_age_25_34' then prevyearvalue else null end ) sessions_with_age_25_34_PrevyearsameX
       
                ,
        
        
           max(case when KPI='age_sessions' then Value else null end ) age_sessions,
           max(case when KPI='age_sessions' then prevValue else null end ) age_sessions_PrevX,
           max(case when KPI='age_sessions' then prevyearvalue else null end ) age_sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'age_35_44_sessions_per' KPI,
date, XTD,
ifnull(safe_divide(sessions_with_age_35_44,age_sessions),0) value,
ifnull(safe_divide(sessions_with_age_35_44_prevX,age_sessions_prevX),0) prevvalue,
ifnull(safe_divide(sessions_with_age_35_44_PrevyearsameX,age_sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='sessions_with_age_35_44' then Value else null end ) sessions_with_age_35_44,
           max(case when KPI='sessions_with_age_35_44' then prevValue else null end ) sessions_with_age_35_44_PrevX,
           max(case when KPI='sessions_with_age_35_44' then prevyearvalue else null end ) sessions_with_age_35_44_PrevyearsameX
       
                ,
        
        
           max(case when KPI='age_sessions' then Value else null end ) age_sessions,
           max(case when KPI='age_sessions' then prevValue else null end ) age_sessions_PrevX,
           max(case when KPI='age_sessions' then prevyearvalue else null end ) age_sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'age_above_45_sessions_per' KPI,
date, XTD,
ifnull(safe_divide(sessions_with_age_above_45,age_sessions),0) value,
ifnull(safe_divide(sessions_with_age_above_45_prevX,age_sessions_prevX),0) prevvalue,
ifnull(safe_divide(sessions_with_age_above_45_PrevyearsameX,age_sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='sessions_with_age_above_45' then Value else null end ) sessions_with_age_above_45,
           max(case when KPI='sessions_with_age_above_45' then prevValue else null end ) sessions_with_age_above_45_PrevX,
           max(case when KPI='sessions_with_age_above_45' then prevyearvalue else null end ) sessions_with_age_above_45_PrevyearsameX
       
                ,
        
        
           max(case when KPI='age_sessions' then Value else null end ) age_sessions,
           max(case when KPI='age_sessions' then prevValue else null end ) age_sessions_PrevX,
           max(case when KPI='age_sessions' then prevyearvalue else null end ) age_sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'gender_male_sessions_per' KPI,
date, XTD,
ifnull(safe_divide(sessions_with_gender_male,gender_sessions),0) value,
ifnull(safe_divide(sessions_with_gender_male_prevX,gender_sessions_prevX),0) prevvalue,
ifnull(safe_divide(sessions_with_gender_male_PrevyearsameX,gender_sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='sessions_with_gender_male' then Value else null end ) sessions_with_gender_male,
           max(case when KPI='sessions_with_gender_male' then prevValue else null end ) sessions_with_gender_male_PrevX,
           max(case when KPI='sessions_with_gender_male' then prevyearvalue else null end ) sessions_with_gender_male_PrevyearsameX
       
                ,
        
        
           max(case when KPI='gender_sessions' then Value else null end ) gender_sessions,
           max(case when KPI='gender_sessions' then prevValue else null end ) gender_sessions_PrevX,
           max(case when KPI='gender_sessions' then prevyearvalue else null end ) gender_sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'gender_female_sessions_per' KPI,
date, XTD,
ifnull(safe_divide(sessions_with_gender_female,gender_sessions),0) value,
ifnull(safe_divide(sessions_with_gender_female_prevX,gender_sessions_prevX),0) prevvalue,
ifnull(safe_divide(sessions_with_gender_female_PrevyearsameX,gender_sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='sessions_with_gender_female' then Value else null end ) sessions_with_gender_female,
           max(case when KPI='sessions_with_gender_female' then prevValue else null end ) sessions_with_gender_female_PrevX,
           max(case when KPI='sessions_with_gender_female' then prevyearvalue else null end ) sessions_with_gender_female_PrevyearsameX
       
                ,
        
        
           max(case when KPI='gender_sessions' then Value else null end ) gender_sessions,
           max(case when KPI='gender_sessions' then prevValue else null end ) gender_sessions_PrevX,
           max(case when KPI='gender_sessions' then prevyearvalue else null end ) gender_sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Snapchat_paid_newusers_per' KPI,
date, XTD,
ifnull(safe_divide(Snapchat_paid_newusers,newuserssourcemedium),0) value,
ifnull(safe_divide(Snapchat_paid_newusers_prevX,newuserssourcemedium_prevX),0) prevvalue,
ifnull(safe_divide(Snapchat_paid_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Snapchat_paid_newusers' then Value else null end ) Snapchat_paid_newusers,
           max(case when KPI='Snapchat_paid_newusers' then prevValue else null end ) Snapchat_paid_newusers_PrevX,
           max(case when KPI='Snapchat_paid_newusers' then prevyearvalue else null end ) Snapchat_paid_newusers_PrevyearsameX
       
                ,
        
        
           max(case when KPI='newuserssourcemedium' then Value else null end ) newuserssourcemedium,
           max(case when KPI='newuserssourcemedium' then prevValue else null end ) newuserssourcemedium_PrevX,
           max(case when KPI='newuserssourcemedium' then prevyearvalue else null end ) newuserssourcemedium_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Snapchat_unpaid_newusers_per' KPI,
date, XTD,
ifnull(safe_divide(Snapchat_unpaid_newusers,newuserssourcemedium),0) value,
ifnull(safe_divide(Snapchat_unpaid_newusers_prevX,newuserssourcemedium_prevX),0) prevvalue,
ifnull(safe_divide(Snapchat_unpaid_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Snapchat_unpaid_newusers' then Value else null end ) Snapchat_unpaid_newusers,
           max(case when KPI='Snapchat_unpaid_newusers' then prevValue else null end ) Snapchat_unpaid_newusers_PrevX,
           max(case when KPI='Snapchat_unpaid_newusers' then prevyearvalue else null end ) Snapchat_unpaid_newusers_PrevyearsameX
       
                ,
        
        
           max(case when KPI='newuserssourcemedium' then Value else null end ) newuserssourcemedium,
           max(case when KPI='newuserssourcemedium' then prevValue else null end ) newuserssourcemedium_PrevX,
           max(case when KPI='newuserssourcemedium' then prevyearvalue else null end ) newuserssourcemedium_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Facebook_Inhouse_Paid_newusers_per' KPI,
date, XTD,
ifnull(safe_divide(Facebook_Inhouse_Paid_newusers,newuserssourcemedium),0) value,
ifnull(safe_divide(Facebook_Inhouse_Paid_newusers_prevX,newuserssourcemedium_prevX),0) prevvalue,
ifnull(safe_divide(Facebook_Inhouse_Paid_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Facebook_Inhouse_Paid_newusers' then Value else null end ) Facebook_Inhouse_Paid_newusers,
           max(case when KPI='Facebook_Inhouse_Paid_newusers' then prevValue else null end ) Facebook_Inhouse_Paid_newusers_PrevX,
           max(case when KPI='Facebook_Inhouse_Paid_newusers' then prevyearvalue else null end ) Facebook_Inhouse_Paid_newusers_PrevyearsameX
       
                ,
        
        
           max(case when KPI='newuserssourcemedium' then Value else null end ) newuserssourcemedium,
           max(case when KPI='newuserssourcemedium' then prevValue else null end ) newuserssourcemedium_PrevX,
           max(case when KPI='newuserssourcemedium' then prevyearvalue else null end ) newuserssourcemedium_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Facebook_Dynamic_Remarketing_newusers_per' KPI,
date, XTD,
ifnull(safe_divide(Facebook_Dynamic_Remarketing_newusers,newuserssourcemedium),0) value,
ifnull(safe_divide(Facebook_Dynamic_Remarketing_newusers_prevX,newuserssourcemedium_prevX),0) prevvalue,
ifnull(safe_divide(Facebook_Dynamic_Remarketing_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Facebook_Dynamic_Remarketing_newusers' then Value else null end ) Facebook_Dynamic_Remarketing_newusers,
           max(case when KPI='Facebook_Dynamic_Remarketing_newusers' then prevValue else null end ) Facebook_Dynamic_Remarketing_newusers_PrevX,
           max(case when KPI='Facebook_Dynamic_Remarketing_newusers' then prevyearvalue else null end ) Facebook_Dynamic_Remarketing_newusers_PrevyearsameX
       
                ,
        
        
           max(case when KPI='newuserssourcemedium' then Value else null end ) newuserssourcemedium,
           max(case when KPI='newuserssourcemedium' then prevValue else null end ) newuserssourcemedium_PrevX,
           max(case when KPI='newuserssourcemedium' then prevyearvalue else null end ) newuserssourcemedium_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Facebook_Unpaid_newusers_per' KPI,
date, XTD,
ifnull(safe_divide(Facebook_Unpaid_newusers,newuserssourcemedium),0) value,
ifnull(safe_divide(Facebook_Unpaid_newusers_prevX,newuserssourcemedium_prevX),0) prevvalue,
ifnull(safe_divide(Facebook_Unpaid_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Facebook_Unpaid_newusers' then Value else null end ) Facebook_Unpaid_newusers,
           max(case when KPI='Facebook_Unpaid_newusers' then prevValue else null end ) Facebook_Unpaid_newusers_PrevX,
           max(case when KPI='Facebook_Unpaid_newusers' then prevyearvalue else null end ) Facebook_Unpaid_newusers_PrevyearsameX
       
                ,
        
        
           max(case when KPI='newuserssourcemedium' then Value else null end ) newuserssourcemedium,
           max(case when KPI='newuserssourcemedium' then prevValue else null end ) newuserssourcemedium_PrevX,
           max(case when KPI='newuserssourcemedium' then prevyearvalue else null end ) newuserssourcemedium_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Instagram_paid_newusers_per' KPI,
date, XTD,
ifnull(safe_divide(Instagram_paid_newusers,newuserssourcemedium),0) value,
ifnull(safe_divide(Instagram_paid_newusers_prevX,newuserssourcemedium_prevX),0) prevvalue,
ifnull(safe_divide(Instagram_paid_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Instagram_paid_newusers' then Value else null end ) Instagram_paid_newusers,
           max(case when KPI='Instagram_paid_newusers' then prevValue else null end ) Instagram_paid_newusers_PrevX,
           max(case when KPI='Instagram_paid_newusers' then prevyearvalue else null end ) Instagram_paid_newusers_PrevyearsameX
       
                ,
        
        
           max(case when KPI='newuserssourcemedium' then Value else null end ) newuserssourcemedium,
           max(case when KPI='newuserssourcemedium' then prevValue else null end ) newuserssourcemedium_PrevX,
           max(case when KPI='newuserssourcemedium' then prevyearvalue else null end ) newuserssourcemedium_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Instagram_Unpaid_newusers_per' KPI,
date, XTD,
ifnull(safe_divide(Instagram_Unpaid_newusers,newuserssourcemedium),0) value,
ifnull(safe_divide(Instagram_Unpaid_newusers_prevX,newuserssourcemedium_prevX),0) prevvalue,
ifnull(safe_divide(Instagram_Unpaid_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Instagram_Unpaid_newusers' then Value else null end ) Instagram_Unpaid_newusers,
           max(case when KPI='Instagram_Unpaid_newusers' then prevValue else null end ) Instagram_Unpaid_newusers_PrevX,
           max(case when KPI='Instagram_Unpaid_newusers' then prevyearvalue else null end ) Instagram_Unpaid_newusers_PrevyearsameX
       
                ,
        
        
           max(case when KPI='newuserssourcemedium' then Value else null end ) newuserssourcemedium,
           max(case when KPI='newuserssourcemedium' then prevValue else null end ) newuserssourcemedium_PrevX,
           max(case when KPI='newuserssourcemedium' then prevyearvalue else null end ) newuserssourcemedium_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Others_newusers_per' KPI,
date, XTD,
ifnull(safe_divide(Others_newusers,newuserssourcemedium),0) value,
ifnull(safe_divide(Others_newusers_prevX,newuserssourcemedium_prevX),0) prevvalue,
ifnull(safe_divide(Others_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Others_newusers' then Value else null end ) Others_newusers,
           max(case when KPI='Others_newusers' then prevValue else null end ) Others_newusers_PrevX,
           max(case when KPI='Others_newusers' then prevyearvalue else null end ) Others_newusers_PrevyearsameX
       
                ,
        
        
           max(case when KPI='newuserssourcemedium' then Value else null end ) newuserssourcemedium,
           max(case when KPI='newuserssourcemedium' then prevValue else null end ) newuserssourcemedium_PrevX,
           max(case when KPI='newuserssourcemedium' then prevyearvalue else null end ) newuserssourcemedium_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'CostperorderMissL10_per' KPI,
date, XTD,
ifnull(safe_divide(MissL10_spend_day,MissL10_orders),0) value,
ifnull(safe_divide(MissL10_spend_day_prevX,MissL10_orders_prevX),0) prevvalue,
ifnull(safe_divide(MissL10_spend_day_PrevyearsameX,MissL10_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='MissL10_spend_day' then Value else null end ) MissL10_spend_day,
           max(case when KPI='MissL10_spend_day' then prevValue else null end ) MissL10_spend_day_PrevX,
           max(case when KPI='MissL10_spend_day' then prevyearvalue else null end ) MissL10_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='MissL10_orders' then Value else null end ) MissL10_orders,
           max(case when KPI='MissL10_orders' then prevValue else null end ) MissL10_orders_PrevX,
           max(case when KPI='MissL10_orders' then prevyearvalue else null end ) MissL10_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'CostperorderHusin10_per' KPI,
date, XTD,
ifnull(safe_divide(Husin10_spend_day,Husin10_orders),0) value,
ifnull(safe_divide(Husin10_spend_day_prevX,Husin10_orders_prevX),0) prevvalue,
ifnull(safe_divide(Husin10_spend_day_PrevyearsameX,Husin10_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Husin10_spend_day' then Value else null end ) Husin10_spend_day,
           max(case when KPI='Husin10_spend_day' then prevValue else null end ) Husin10_spend_day_PrevX,
           max(case when KPI='Husin10_spend_day' then prevyearvalue else null end ) Husin10_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Husin10_orders' then Value else null end ) Husin10_orders,
           max(case when KPI='Husin10_orders' then prevValue else null end ) Husin10_orders_PrevX,
           max(case when KPI='Husin10_orders' then prevyearvalue else null end ) Husin10_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'CostperorderFeda10_per' KPI,
date, XTD,
ifnull(safe_divide(Feda10_spend_day,Feda10_orders),0) value,
ifnull(safe_divide(Feda10_spend_day_prevX,Feda10_orders_prevX),0) prevvalue,
ifnull(safe_divide(Feda10_spend_day_PrevyearsameX,Feda10_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Feda10_spend_day' then Value else null end ) Feda10_spend_day,
           max(case when KPI='Feda10_spend_day' then prevValue else null end ) Feda10_spend_day_PrevX,
           max(case when KPI='Feda10_spend_day' then prevyearvalue else null end ) Feda10_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Feda10_orders' then Value else null end ) Feda10_orders,
           max(case when KPI='Feda10_orders' then prevValue else null end ) Feda10_orders_PrevX,
           max(case when KPI='Feda10_orders' then prevyearvalue else null end ) Feda10_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'CostperorderAbrar21_per' KPI,
date, XTD,
ifnull(safe_divide(abrar21_spend_day,abrar21_orders),0) value,
ifnull(safe_divide(abrar21_spend_day_prevX,abrar21_orders_prevX),0) prevvalue,
ifnull(safe_divide(abrar21_spend_day_PrevyearsameX,abrar21_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='abrar21_spend_day' then Value else null end ) abrar21_spend_day,
           max(case when KPI='abrar21_spend_day' then prevValue else null end ) abrar21_spend_day_PrevX,
           max(case when KPI='abrar21_spend_day' then prevyearvalue else null end ) abrar21_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='abrar21_orders' then Value else null end ) abrar21_orders,
           max(case when KPI='abrar21_orders' then prevValue else null end ) abrar21_orders_PrevX,
           max(case when KPI='abrar21_orders' then prevyearvalue else null end ) abrar21_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Costperorderfeda21_per' KPI,
date, XTD,
ifnull(safe_divide(feda21_spend_day,feda21_orders),0) value,
ifnull(safe_divide(feda21_spend_day_prevX,feda21_orders_prevX),0) prevvalue,
ifnull(safe_divide(feda21_spend_day_PrevyearsameX,feda21_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='feda21_spend_day' then Value else null end ) feda21_spend_day,
           max(case when KPI='feda21_spend_day' then prevValue else null end ) feda21_spend_day_PrevX,
           max(case when KPI='feda21_spend_day' then prevyearvalue else null end ) feda21_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='feda21_orders' then Value else null end ) feda21_orders,
           max(case when KPI='feda21_orders' then prevValue else null end ) feda21_orders_PrevX,
           max(case when KPI='feda21_orders' then prevyearvalue else null end ) feda21_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'CostperorderRohN2_per' KPI,
date, XTD,
ifnull(safe_divide(roh_n2_spend_day,roh_n2_orders),0) value,
ifnull(safe_divide(roh_n2_spend_day_prevX,roh_n2_orders_prevX),0) prevvalue,
ifnull(safe_divide(roh_n2_spend_day_PrevyearsameX,roh_n2_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='roh_n2_spend_day' then Value else null end ) roh_n2_spend_day,
           max(case when KPI='roh_n2_spend_day' then prevValue else null end ) roh_n2_spend_day_PrevX,
           max(case when KPI='roh_n2_spend_day' then prevyearvalue else null end ) roh_n2_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='roh_n2_orders' then Value else null end ) roh_n2_orders,
           max(case when KPI='roh_n2_orders' then prevValue else null end ) roh_n2_orders_PrevX,
           max(case when KPI='roh_n2_orders' then prevyearvalue else null end ) roh_n2_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Costperorderomseba10_per' KPI,
date, XTD,
ifnull(safe_divide(omseba10_spend_day,omseba10_orders),0) value,
ifnull(safe_divide(omseba10_spend_day_prevX,omseba10_orders_prevX),0) prevvalue,
ifnull(safe_divide(omseba10_spend_day_PrevyearsameX,omseba10_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='omseba10_spend_day' then Value else null end ) omseba10_spend_day,
           max(case when KPI='omseba10_spend_day' then prevValue else null end ) omseba10_spend_day_PrevX,
           max(case when KPI='omseba10_spend_day' then prevyearvalue else null end ) omseba10_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='omseba10_orders' then Value else null end ) omseba10_orders,
           max(case when KPI='omseba10_orders' then prevValue else null end ) omseba10_orders_PrevX,
           max(case when KPI='omseba10_orders' then prevyearvalue else null end ) omseba10_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Costperordermarwa10_per' KPI,
date, XTD,
ifnull(safe_divide(marwa10_spend_day,marwa10_orders),0) value,
ifnull(safe_divide(marwa10_spend_day_prevX,marwa10_orders_prevX),0) prevvalue,
ifnull(safe_divide(marwa10_spend_day_PrevyearsameX,marwa10_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='marwa10_spend_day' then Value else null end ) marwa10_spend_day,
           max(case when KPI='marwa10_spend_day' then prevValue else null end ) marwa10_spend_day_PrevX,
           max(case when KPI='marwa10_spend_day' then prevyearvalue else null end ) marwa10_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='marwa10_orders' then Value else null end ) marwa10_orders,
           max(case when KPI='marwa10_orders' then prevValue else null end ) marwa10_orders_PrevX,
           max(case when KPI='marwa10_orders' then prevyearvalue else null end ) marwa10_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'CostperorderMaha140_per' KPI,
date, XTD,
ifnull(safe_divide(maha_140_spend_day,maha_140_orders),0) value,
ifnull(safe_divide(maha_140_spend_day_prevX,maha_140_orders_prevX),0) prevvalue,
ifnull(safe_divide(maha_140_spend_day_PrevyearsameX,maha_140_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='maha_140_spend_day' then Value else null end ) maha_140_spend_day,
           max(case when KPI='maha_140_spend_day' then prevValue else null end ) maha_140_spend_day_PrevX,
           max(case when KPI='maha_140_spend_day' then prevyearvalue else null end ) maha_140_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='maha_140_orders' then Value else null end ) maha_140_orders,
           max(case when KPI='maha_140_orders' then prevValue else null end ) maha_140_orders_PrevX,
           max(case when KPI='maha_140_orders' then prevyearvalue else null end ) maha_140_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'CostperorderShahad10_per' KPI,
date, XTD,
ifnull(safe_divide(shahad10_spend_day,shahad10_orders),0) value,
ifnull(safe_divide(shahad10_spend_day_prevX,shahad10_orders_prevX),0) prevvalue,
ifnull(safe_divide(shahad10_spend_day_PrevyearsameX,shahad10_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='shahad10_spend_day' then Value else null end ) shahad10_spend_day,
           max(case when KPI='shahad10_spend_day' then prevValue else null end ) shahad10_spend_day_PrevX,
           max(case when KPI='shahad10_spend_day' then prevyearvalue else null end ) shahad10_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='shahad10_orders' then Value else null end ) shahad10_orders,
           max(case when KPI='shahad10_orders' then prevValue else null end ) shahad10_orders_PrevX,
           max(case when KPI='shahad10_orders' then prevyearvalue else null end ) shahad10_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ATV Customer Acq MissL' KPI,
date, XTD,
ifnull(safe_divide(MissL10_revenue,MissL10_orders),0) value,
ifnull(safe_divide(MissL10_revenue_prevX,MissL10_orders_prevX),0) prevvalue,
ifnull(safe_divide(MissL10_revenue_PrevyearsameX,MissL10_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='MissL10_revenue' then Value else null end ) MissL10_revenue,
           max(case when KPI='MissL10_revenue' then prevValue else null end ) MissL10_revenue_PrevX,
           max(case when KPI='MissL10_revenue' then prevyearvalue else null end ) MissL10_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='MissL10_orders' then Value else null end ) MissL10_orders,
           max(case when KPI='MissL10_orders' then prevValue else null end ) MissL10_orders_PrevX,
           max(case when KPI='MissL10_orders' then prevyearvalue else null end ) MissL10_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ATV Customer Acq Husin10' KPI,
date, XTD,
ifnull(safe_divide(Husin10_revenue,Husin10_orders),0) value,
ifnull(safe_divide(Husin10_revenue_prevX,Husin10_orders_prevX),0) prevvalue,
ifnull(safe_divide(Husin10_revenue_PrevyearsameX,Husin10_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Husin10_revenue' then Value else null end ) Husin10_revenue,
           max(case when KPI='Husin10_revenue' then prevValue else null end ) Husin10_revenue_PrevX,
           max(case when KPI='Husin10_revenue' then prevyearvalue else null end ) Husin10_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Husin10_orders' then Value else null end ) Husin10_orders,
           max(case when KPI='Husin10_orders' then prevValue else null end ) Husin10_orders_PrevX,
           max(case when KPI='Husin10_orders' then prevyearvalue else null end ) Husin10_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ATV Customer Acq Feda10' KPI,
date, XTD,
ifnull(safe_divide(Feda10_revenue,Feda10_orders),0) value,
ifnull(safe_divide(Feda10_revenue_prevX,Feda10_orders_prevX),0) prevvalue,
ifnull(safe_divide(Feda10_revenue_PrevyearsameX,Feda10_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Feda10_revenue' then Value else null end ) Feda10_revenue,
           max(case when KPI='Feda10_revenue' then prevValue else null end ) Feda10_revenue_PrevX,
           max(case when KPI='Feda10_revenue' then prevyearvalue else null end ) Feda10_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Feda10_orders' then Value else null end ) Feda10_orders,
           max(case when KPI='Feda10_orders' then prevValue else null end ) Feda10_orders_PrevX,
           max(case when KPI='Feda10_orders' then prevyearvalue else null end ) Feda10_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ATV Customer Acq Abrar21' KPI,
date, XTD,
ifnull(safe_divide(abrar21_revenue,abrar21_orders),0) value,
ifnull(safe_divide(abrar21_revenue_prevX,abrar21_orders_prevX),0) prevvalue,
ifnull(safe_divide(abrar21_revenue_PrevyearsameX,abrar21_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='abrar21_revenue' then Value else null end ) abrar21_revenue,
           max(case when KPI='abrar21_revenue' then prevValue else null end ) abrar21_revenue_PrevX,
           max(case when KPI='abrar21_revenue' then prevyearvalue else null end ) abrar21_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='abrar21_orders' then Value else null end ) abrar21_orders,
           max(case when KPI='abrar21_orders' then prevValue else null end ) abrar21_orders_PrevX,
           max(case when KPI='abrar21_orders' then prevyearvalue else null end ) abrar21_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ATV Customer Acq Feda21' KPI,
date, XTD,
ifnull(safe_divide(feda21_revenue,feda21_orders),0) value,
ifnull(safe_divide(feda21_revenue_prevX,feda21_orders_prevX),0) prevvalue,
ifnull(safe_divide(feda21_revenue_PrevyearsameX,feda21_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='feda21_revenue' then Value else null end ) feda21_revenue,
           max(case when KPI='feda21_revenue' then prevValue else null end ) feda21_revenue_PrevX,
           max(case when KPI='feda21_revenue' then prevyearvalue else null end ) feda21_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='feda21_orders' then Value else null end ) feda21_orders,
           max(case when KPI='feda21_orders' then prevValue else null end ) feda21_orders_PrevX,
           max(case when KPI='feda21_orders' then prevyearvalue else null end ) feda21_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ATV Customer Acq RohN2' KPI,
date, XTD,
ifnull(safe_divide(roh_n2_revenue,roh_n2_orders),0) value,
ifnull(safe_divide(roh_n2_revenue_prevX,roh_n2_orders_prevX),0) prevvalue,
ifnull(safe_divide(roh_n2_revenue_PrevyearsameX,roh_n2_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='roh_n2_revenue' then Value else null end ) roh_n2_revenue,
           max(case when KPI='roh_n2_revenue' then prevValue else null end ) roh_n2_revenue_PrevX,
           max(case when KPI='roh_n2_revenue' then prevyearvalue else null end ) roh_n2_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='roh_n2_orders' then Value else null end ) roh_n2_orders,
           max(case when KPI='roh_n2_orders' then prevValue else null end ) roh_n2_orders_PrevX,
           max(case when KPI='roh_n2_orders' then prevyearvalue else null end ) roh_n2_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ATV Customer Acq Omseba10' KPI,
date, XTD,
ifnull(safe_divide(omseba10_revenue,omseba10_orders),0) value,
ifnull(safe_divide(omseba10_revenue_prevX,omseba10_orders_prevX),0) prevvalue,
ifnull(safe_divide(omseba10_revenue_PrevyearsameX,omseba10_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='omseba10_revenue' then Value else null end ) omseba10_revenue,
           max(case when KPI='omseba10_revenue' then prevValue else null end ) omseba10_revenue_PrevX,
           max(case when KPI='omseba10_revenue' then prevyearvalue else null end ) omseba10_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='omseba10_orders' then Value else null end ) omseba10_orders,
           max(case when KPI='omseba10_orders' then prevValue else null end ) omseba10_orders_PrevX,
           max(case when KPI='omseba10_orders' then prevyearvalue else null end ) omseba10_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ATV Customer Acq Marwa10' KPI,
date, XTD,
ifnull(safe_divide(marwa10_revenue,marwa10_orders),0) value,
ifnull(safe_divide(marwa10_revenue_prevX,marwa10_orders_prevX),0) prevvalue,
ifnull(safe_divide(marwa10_revenue_PrevyearsameX,marwa10_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='marwa10_revenue' then Value else null end ) marwa10_revenue,
           max(case when KPI='marwa10_revenue' then prevValue else null end ) marwa10_revenue_PrevX,
           max(case when KPI='marwa10_revenue' then prevyearvalue else null end ) marwa10_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='marwa10_orders' then Value else null end ) marwa10_orders,
           max(case when KPI='marwa10_orders' then prevValue else null end ) marwa10_orders_PrevX,
           max(case when KPI='marwa10_orders' then prevyearvalue else null end ) marwa10_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ATV Customer Acq Maha140' KPI,
date, XTD,
ifnull(safe_divide(maha_140_revenue,maha_140_orders),0) value,
ifnull(safe_divide(maha_140_revenue_prevX,maha_140_orders_prevX),0) prevvalue,
ifnull(safe_divide(maha_140_revenue_PrevyearsameX,maha_140_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='maha_140_revenue' then Value else null end ) maha_140_revenue,
           max(case when KPI='maha_140_revenue' then prevValue else null end ) maha_140_revenue_PrevX,
           max(case when KPI='maha_140_revenue' then prevyearvalue else null end ) maha_140_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='maha_140_orders' then Value else null end ) maha_140_orders,
           max(case when KPI='maha_140_orders' then prevValue else null end ) maha_140_orders_PrevX,
           max(case when KPI='maha_140_orders' then prevyearvalue else null end ) maha_140_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ATV Customer Acq Shahad10' KPI,
date, XTD,
ifnull(safe_divide(shahad10_revenue,shahad10_orders),0) value,
ifnull(safe_divide(shahad10_revenue_prevX,shahad10_orders_prevX),0) prevvalue,
ifnull(safe_divide(shahad10_revenue_PrevyearsameX,shahad10_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='shahad10_revenue' then Value else null end ) shahad10_revenue,
           max(case when KPI='shahad10_revenue' then prevValue else null end ) shahad10_revenue_PrevX,
           max(case when KPI='shahad10_revenue' then prevyearvalue else null end ) shahad10_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='shahad10_orders' then Value else null end ) shahad10_orders,
           max(case when KPI='shahad10_orders' then prevValue else null end ) shahad10_orders_PrevX,
           max(case when KPI='shahad10_orders' then prevyearvalue else null end ) shahad10_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Shareofsales_per MissL10' KPI,
date, XTD,
ifnull(safe_divide(MissL10_revenue,total_item_price_auth),0) value,
ifnull(safe_divide(MissL10_revenue_prevX,total_item_price_auth_prevX),0) prevvalue,
ifnull(safe_divide(MissL10_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='MissL10_revenue' then Value else null end ) MissL10_revenue,
           max(case when KPI='MissL10_revenue' then prevValue else null end ) MissL10_revenue_PrevX,
           max(case when KPI='MissL10_revenue' then prevyearvalue else null end ) MissL10_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Shareofsales_per Husin10' KPI,
date, XTD,
ifnull(safe_divide(Husin10_revenue,total_item_price_auth),0) value,
ifnull(safe_divide(Husin10_revenue_prevX,total_item_price_auth_prevX),0) prevvalue,
ifnull(safe_divide(Husin10_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Husin10_revenue' then Value else null end ) Husin10_revenue,
           max(case when KPI='Husin10_revenue' then prevValue else null end ) Husin10_revenue_PrevX,
           max(case when KPI='Husin10_revenue' then prevyearvalue else null end ) Husin10_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Shareofsales_per Feda10' KPI,
date, XTD,
ifnull(safe_divide(Feda10_revenue,total_item_price_auth),0) value,
ifnull(safe_divide(Feda10_revenue_prevX,total_item_price_auth_prevX),0) prevvalue,
ifnull(safe_divide(Feda10_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Feda10_revenue' then Value else null end ) Feda10_revenue,
           max(case when KPI='Feda10_revenue' then prevValue else null end ) Feda10_revenue_PrevX,
           max(case when KPI='Feda10_revenue' then prevyearvalue else null end ) Feda10_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Shareofsales_per Abrar21' KPI,
date, XTD,
ifnull(safe_divide(abrar21_revenue,total_item_price_auth),0) value,
ifnull(safe_divide(abrar21_revenue_prevX,total_item_price_auth_prevX),0) prevvalue,
ifnull(safe_divide(abrar21_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='abrar21_revenue' then Value else null end ) abrar21_revenue,
           max(case when KPI='abrar21_revenue' then prevValue else null end ) abrar21_revenue_PrevX,
           max(case when KPI='abrar21_revenue' then prevyearvalue else null end ) abrar21_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Shareofsales_per Feda21' KPI,
date, XTD,
ifnull(safe_divide(feda21_revenue,total_item_price_auth),0) value,
ifnull(safe_divide(feda21_revenue_prevX,total_item_price_auth_prevX),0) prevvalue,
ifnull(safe_divide(feda21_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='feda21_revenue' then Value else null end ) feda21_revenue,
           max(case when KPI='feda21_revenue' then prevValue else null end ) feda21_revenue_PrevX,
           max(case when KPI='feda21_revenue' then prevyearvalue else null end ) feda21_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Shareofsales_per RohN2' KPI,
date, XTD,
ifnull(safe_divide(roh_n2_revenue,total_item_price_auth),0) value,
ifnull(safe_divide(roh_n2_revenue_prevX,total_item_price_auth_prevX),0) prevvalue,
ifnull(safe_divide(roh_n2_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='roh_n2_revenue' then Value else null end ) roh_n2_revenue,
           max(case when KPI='roh_n2_revenue' then prevValue else null end ) roh_n2_revenue_PrevX,
           max(case when KPI='roh_n2_revenue' then prevyearvalue else null end ) roh_n2_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Shareofsales_per Omseba10' KPI,
date, XTD,
ifnull(safe_divide(omseba10_revenue,total_item_price_auth),0) value,
ifnull(safe_divide(omseba10_revenue_prevX,total_item_price_auth_prevX),0) prevvalue,
ifnull(safe_divide(omseba10_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='omseba10_revenue' then Value else null end ) omseba10_revenue,
           max(case when KPI='omseba10_revenue' then prevValue else null end ) omseba10_revenue_PrevX,
           max(case when KPI='omseba10_revenue' then prevyearvalue else null end ) omseba10_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Shareofsales_per Marwa10' KPI,
date, XTD,
ifnull(safe_divide(marwa10_revenue,total_item_price_auth),0) value,
ifnull(safe_divide(marwa10_revenue_prevX,total_item_price_auth_prevX),0) prevvalue,
ifnull(safe_divide(marwa10_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='marwa10_revenue' then Value else null end ) marwa10_revenue,
           max(case when KPI='marwa10_revenue' then prevValue else null end ) marwa10_revenue_PrevX,
           max(case when KPI='marwa10_revenue' then prevyearvalue else null end ) marwa10_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Shareofsales_per Maha140' KPI,
date, XTD,
ifnull(safe_divide(maha_140_revenue,total_item_price_auth),0) value,
ifnull(safe_divide(maha_140_revenue_prevX,total_item_price_auth_prevX),0) prevvalue,
ifnull(safe_divide(maha_140_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='maha_140_revenue' then Value else null end ) maha_140_revenue,
           max(case when KPI='maha_140_revenue' then prevValue else null end ) maha_140_revenue_PrevX,
           max(case when KPI='maha_140_revenue' then prevyearvalue else null end ) maha_140_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Shareofsales_per Shahad10' KPI,
date, XTD,
ifnull(safe_divide(shahad10_revenue,total_item_price_auth),0) value,
ifnull(safe_divide(shahad10_revenue_prevX,total_item_price_auth_prevX),0) prevvalue,
ifnull(safe_divide(shahad10_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='shahad10_revenue' then Value else null end ) shahad10_revenue,
           max(case when KPI='shahad10_revenue' then prevValue else null end ) shahad10_revenue_PrevX,
           max(case when KPI='shahad10_revenue' then prevyearvalue else null end ) shahad10_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Costperorder_Facebook' KPI,
date, XTD,
ifnull(safe_divide(Facebook_cust_spend,Facebook_cust_orders),0) value,
ifnull(safe_divide(Facebook_cust_spend_prevX,Facebook_cust_orders_prevX),0) prevvalue,
ifnull(safe_divide(Facebook_cust_spend_PrevyearsameX,Facebook_cust_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Facebook_cust_spend' then Value else null end ) Facebook_cust_spend,
           max(case when KPI='Facebook_cust_spend' then prevValue else null end ) Facebook_cust_spend_PrevX,
           max(case when KPI='Facebook_cust_spend' then prevyearvalue else null end ) Facebook_cust_spend_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Facebook_cust_orders' then Value else null end ) Facebook_cust_orders,
           max(case when KPI='Facebook_cust_orders' then prevValue else null end ) Facebook_cust_orders_PrevX,
           max(case when KPI='Facebook_cust_orders' then prevyearvalue else null end ) Facebook_cust_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Costperorder_Instagram' KPI,
date, XTD,
ifnull(safe_divide(Instagram_cust_spend,Instagram_cust_orders),0) value,
ifnull(safe_divide(Instagram_cust_spend_prevX,Instagram_cust_orders_prevX),0) prevvalue,
ifnull(safe_divide(Instagram_cust_spend_PrevyearsameX,Instagram_cust_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Instagram_cust_spend' then Value else null end ) Instagram_cust_spend,
           max(case when KPI='Instagram_cust_spend' then prevValue else null end ) Instagram_cust_spend_PrevX,
           max(case when KPI='Instagram_cust_spend' then prevyearvalue else null end ) Instagram_cust_spend_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Instagram_cust_orders' then Value else null end ) Instagram_cust_orders,
           max(case when KPI='Instagram_cust_orders' then prevValue else null end ) Instagram_cust_orders_PrevX,
           max(case when KPI='Instagram_cust_orders' then prevyearvalue else null end ) Instagram_cust_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Costperorder_Snapchat' KPI,
date, XTD,
ifnull(safe_divide(snapchat_cust_spend,Snapchat_cust_orders),0) value,
ifnull(safe_divide(snapchat_cust_spend_prevX,Snapchat_cust_orders_prevX),0) prevvalue,
ifnull(safe_divide(snapchat_cust_spend_PrevyearsameX,snapchat_cust_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='snapchat_cust_spend' then Value else null end ) snapchat_cust_spend,
           max(case when KPI='snapchat_cust_spend' then prevValue else null end ) snapchat_cust_spend_PrevX,
           max(case when KPI='snapchat_cust_spend' then prevyearvalue else null end ) snapchat_cust_spend_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Snapchat_cust_orders' then Value else null end ) Snapchat_cust_orders,
           max(case when KPI='Snapchat_cust_orders' then prevValue else null end ) Snapchat_cust_orders_PrevX,
           max(case when KPI='Snapchat_cust_orders' then prevyearvalue else null end ) Snapchat_cust_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Costperorder_Adwords' KPI,
date, XTD,
ifnull(safe_divide(google_cust_adspend,Adwords_cust_orders),0) value,
ifnull(safe_divide(google_cust_adspend_prevX,Adwords_cust_orders_prevX),0) prevvalue,
ifnull(safe_divide(google_cust_adspend_PrevyearsameX,Adwords_cust_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='google_cust_adspend' then Value else null end ) google_cust_adspend,
           max(case when KPI='google_cust_adspend' then prevValue else null end ) google_cust_adspend_PrevX,
           max(case when KPI='google_cust_adspend' then prevyearvalue else null end ) google_cust_adspend_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Adwords_cust_orders' then Value else null end ) Adwords_cust_orders,
           max(case when KPI='Adwords_cust_orders' then prevValue else null end ) Adwords_cust_orders_PrevX,
           max(case when KPI='Adwords_cust_orders' then prevyearvalue else null end ) Adwords_cust_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Costperorder_roi_hunter' KPI,
date, XTD,
ifnull(safe_divide(roi_hunter_cust_spend,Roi_hunter_cust_orders),0) value,
ifnull(safe_divide(roi_hunter_cust_spend_prevX,Roi_hunter_cust_orders_prevX),0) prevvalue,
ifnull(safe_divide(roi_hunter_cust_spend_PrevyearsameX,Roi_hunter_cust_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='roi_hunter_cust_spend' then Value else null end ) roi_hunter_cust_spend,
           max(case when KPI='roi_hunter_cust_spend' then prevValue else null end ) roi_hunter_cust_spend_PrevX,
           max(case when KPI='roi_hunter_cust_spend' then prevyearvalue else null end ) roi_hunter_cust_spend_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Roi_hunter_cust_orders' then Value else null end ) Roi_hunter_cust_orders,
           max(case when KPI='Roi_hunter_cust_orders' then prevValue else null end ) Roi_hunter_cust_orders_PrevX,
           max(case when KPI='Roi_hunter_cust_orders' then prevyearvalue else null end ) Roi_hunter_cust_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'AOV_Facebook' KPI,
date, XTD,
ifnull(safe_divide(Facebook_cust_revenue,Facebook_cust_orders),0) value,
ifnull(safe_divide(Facebook_cust_revenue_prevX,Facebook_cust_orders_prevX),0) prevvalue,
ifnull(safe_divide(Facebook_cust_revenue_PrevyearsameX,Facebook_cust_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Facebook_cust_revenue' then Value else null end ) Facebook_cust_revenue,
           max(case when KPI='Facebook_cust_revenue' then prevValue else null end ) Facebook_cust_revenue_PrevX,
           max(case when KPI='Facebook_cust_revenue' then prevyearvalue else null end ) Facebook_cust_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Facebook_cust_orders' then Value else null end ) Facebook_cust_orders,
           max(case when KPI='Facebook_cust_orders' then prevValue else null end ) Facebook_cust_orders_PrevX,
           max(case when KPI='Facebook_cust_orders' then prevyearvalue else null end ) Facebook_cust_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'AOV_Instagram' KPI,
date, XTD,
ifnull(safe_divide(Instagram_cust_revenue,Instagram_cust_orders),0) value,
ifnull(safe_divide(Instagram_cust_revenue_prevX,Instagram_cust_orders_prevX),0) prevvalue,
ifnull(safe_divide(Instagram_cust_revenue_PrevyearsameX,Instagram_cust_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Instagram_cust_revenue' then Value else null end ) Instagram_cust_revenue,
           max(case when KPI='Instagram_cust_revenue' then prevValue else null end ) Instagram_cust_revenue_PrevX,
           max(case when KPI='Instagram_cust_revenue' then prevyearvalue else null end ) Instagram_cust_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Instagram_cust_orders' then Value else null end ) Instagram_cust_orders,
           max(case when KPI='Instagram_cust_orders' then prevValue else null end ) Instagram_cust_orders_PrevX,
           max(case when KPI='Instagram_cust_orders' then prevyearvalue else null end ) Instagram_cust_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'AOV_Snapchat' KPI,
date, XTD,
ifnull(safe_divide(Snapchat_cust_revenue,Snapchat_cust_orders),0) value,
ifnull(safe_divide(Snapchat_cust_revenue_prevX,Snapchat_cust_orders_prevX),0) prevvalue,
ifnull(safe_divide(Snapchat_cust_revenue_PrevyearsameX,Snapchat_cust_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Snapchat_cust_revenue' then Value else null end ) Snapchat_cust_revenue,
           max(case when KPI='Snapchat_cust_revenue' then prevValue else null end ) Snapchat_cust_revenue_PrevX,
           max(case when KPI='Snapchat_cust_revenue' then prevyearvalue else null end ) Snapchat_cust_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Snapchat_cust_orders' then Value else null end ) Snapchat_cust_orders,
           max(case when KPI='Snapchat_cust_orders' then prevValue else null end ) Snapchat_cust_orders_PrevX,
           max(case when KPI='Snapchat_cust_orders' then prevyearvalue else null end ) Snapchat_cust_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'AOV_Adwords' KPI,
date, XTD,
ifnull(safe_divide(Adwords_cust_revenue,Adwords_cust_orders),0) value,
ifnull(safe_divide(Adwords_cust_revenue_prevX,Adwords_cust_orders_prevX),0) prevvalue,
ifnull(safe_divide(Adwords_cust_revenue_PrevyearsameX,Adwords_cust_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Adwords_cust_revenue' then Value else null end ) Adwords_cust_revenue,
           max(case when KPI='Adwords_cust_revenue' then prevValue else null end ) Adwords_cust_revenue_PrevX,
           max(case when KPI='Adwords_cust_revenue' then prevyearvalue else null end ) Adwords_cust_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Adwords_cust_orders' then Value else null end ) Adwords_cust_orders,
           max(case when KPI='Adwords_cust_orders' then prevValue else null end ) Adwords_cust_orders_PrevX,
           max(case when KPI='Adwords_cust_orders' then prevyearvalue else null end ) Adwords_cust_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'AOV_roi_hunter' KPI,
date, XTD,
ifnull(safe_divide(Roi_hunter_cust_revenue,Roi_hunter_cust_orders),0) value,
ifnull(safe_divide(Roi_hunter_cust_revenue_prevX,Roi_hunter_cust_orders_prevX),0) prevvalue,
ifnull(safe_divide(Roi_hunter_cust_revenue_PrevyearsameX,Roi_hunter_cust_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Roi_hunter_cust_revenue' then Value else null end ) Roi_hunter_cust_revenue,
           max(case when KPI='Roi_hunter_cust_revenue' then prevValue else null end ) Roi_hunter_cust_revenue_PrevX,
           max(case when KPI='Roi_hunter_cust_revenue' then prevyearvalue else null end ) Roi_hunter_cust_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Roi_hunter_cust_orders' then Value else null end ) Roi_hunter_cust_orders,
           max(case when KPI='Roi_hunter_cust_orders' then prevValue else null end ) Roi_hunter_cust_orders_PrevX,
           max(case when KPI='Roi_hunter_cust_orders' then prevyearvalue else null end ) Roi_hunter_cust_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Shareofsales_Facebook' KPI,
date, XTD,
ifnull(safe_divide(Facebook_cust_revenue,total_item_price_auth),0) value,
ifnull(safe_divide(Facebook_cust_revenue_prevX,total_item_price_auth_prevX),0) prevvalue,
ifnull(safe_divide(Facebook_cust_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Facebook_cust_revenue' then Value else null end ) Facebook_cust_revenue,
           max(case when KPI='Facebook_cust_revenue' then prevValue else null end ) Facebook_cust_revenue_PrevX,
           max(case when KPI='Facebook_cust_revenue' then prevyearvalue else null end ) Facebook_cust_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Shareofsales_Instagram' KPI,
date, XTD,
ifnull(safe_divide(Instagram_cust_revenue,total_item_price_auth),0) value,
ifnull(safe_divide(Instagram_cust_revenue_prevX,total_item_price_auth_prevX),0) prevvalue,
ifnull(safe_divide(Instagram_cust_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Instagram_cust_revenue' then Value else null end ) Instagram_cust_revenue,
           max(case when KPI='Instagram_cust_revenue' then prevValue else null end ) Instagram_cust_revenue_PrevX,
           max(case when KPI='Instagram_cust_revenue' then prevyearvalue else null end ) Instagram_cust_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Shareofsales_Snapchat' KPI,
date, XTD,
ifnull(safe_divide(Snapchat_cust_revenue,total_item_price_auth),0) value,
ifnull(safe_divide(Snapchat_cust_revenue_prevX,total_item_price_auth_prevX),0) prevvalue,
ifnull(safe_divide(Snapchat_cust_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Snapchat_cust_revenue' then Value else null end ) Snapchat_cust_revenue,
           max(case when KPI='Snapchat_cust_revenue' then prevValue else null end ) Snapchat_cust_revenue_PrevX,
           max(case when KPI='Snapchat_cust_revenue' then prevyearvalue else null end ) Snapchat_cust_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Shareofsales_Adwords' KPI,
date, XTD,
ifnull(safe_divide(Adwords_cust_revenue,total_item_price_auth),0) value,
ifnull(safe_divide(Adwords_cust_revenue_prevX,total_item_price_auth_prevX),0) prevvalue,
ifnull(safe_divide(Adwords_cust_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Adwords_cust_revenue' then Value else null end ) Adwords_cust_revenue,
           max(case when KPI='Adwords_cust_revenue' then prevValue else null end ) Adwords_cust_revenue_PrevX,
           max(case when KPI='Adwords_cust_revenue' then prevyearvalue else null end ) Adwords_cust_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Sahreofsales_roi_hunter' KPI,
date, XTD,
ifnull(safe_divide(Roi_hunter_cust_revenue,total_item_price_auth),0) value,
ifnull(safe_divide(Roi_hunter_cust_revenue_prevX,total_item_price_auth_prevX),0) prevvalue,
ifnull(safe_divide(Roi_hunter_cust_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Roi_hunter_cust_revenue' then Value else null end ) Roi_hunter_cust_revenue,
           max(case when KPI='Roi_hunter_cust_revenue' then prevValue else null end ) Roi_hunter_cust_revenue_PrevX,
           max(case when KPI='Roi_hunter_cust_revenue' then prevyearvalue else null end ) Roi_hunter_cust_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ConversionRate_Facebook' KPI,
date, XTD,
ifnull(safe_divide(Facebook_cust_orders,Facebook_cust_traffic),0) value,
ifnull(safe_divide(Facebook_cust_orders_prevX,Facebook_cust_traffic_prevX),0) prevvalue,
ifnull(safe_divide(Facebook_cust_orders_PrevyearsameX,Facebook_cust_traffic_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Facebook_cust_orders' then Value else null end ) Facebook_cust_orders,
           max(case when KPI='Facebook_cust_orders' then prevValue else null end ) Facebook_cust_orders_PrevX,
           max(case when KPI='Facebook_cust_orders' then prevyearvalue else null end ) Facebook_cust_orders_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Facebook_cust_traffic' then Value else null end ) Facebook_cust_traffic,
           max(case when KPI='Facebook_cust_traffic' then prevValue else null end ) Facebook_cust_traffic_PrevX,
           max(case when KPI='Facebook_cust_traffic' then prevyearvalue else null end ) Facebook_cust_traffic_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ConversionRate_Instagram' KPI,
date, XTD,
ifnull(safe_divide(Instagram_cust_orders,Instagram_cust_traffic),0) value,
ifnull(safe_divide(Instagram_cust_orders_prevX,Instagram_cust_traffic_prevX),0) prevvalue,
ifnull(safe_divide(Instagram_cust_orders_PrevyearsameX,Instagram_cust_traffic_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Instagram_cust_orders' then Value else null end ) Instagram_cust_orders,
           max(case when KPI='Instagram_cust_orders' then prevValue else null end ) Instagram_cust_orders_PrevX,
           max(case when KPI='Instagram_cust_orders' then prevyearvalue else null end ) Instagram_cust_orders_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Instagram_cust_traffic' then Value else null end ) Instagram_cust_traffic,
           max(case when KPI='Instagram_cust_traffic' then prevValue else null end ) Instagram_cust_traffic_PrevX,
           max(case when KPI='Instagram_cust_traffic' then prevyearvalue else null end ) Instagram_cust_traffic_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ConversionRate_Snapchat' KPI,
date, XTD,
ifnull(safe_divide(Snapchat_cust_orders,Snapchat_cust_traffic),0) value,
ifnull(safe_divide(Snapchat_cust_orders_prevX,Snapchat_cust_traffic_prevX),0) prevvalue,
ifnull(safe_divide(Snapchat_cust_orders_PrevyearsameX,Snapchat_cust_traffic_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Snapchat_cust_orders' then Value else null end ) Snapchat_cust_orders,
           max(case when KPI='Snapchat_cust_orders' then prevValue else null end ) Snapchat_cust_orders_PrevX,
           max(case when KPI='Snapchat_cust_orders' then prevyearvalue else null end ) Snapchat_cust_orders_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Snapchat_cust_traffic' then Value else null end ) Snapchat_cust_traffic,
           max(case when KPI='Snapchat_cust_traffic' then prevValue else null end ) Snapchat_cust_traffic_PrevX,
           max(case when KPI='Snapchat_cust_traffic' then prevyearvalue else null end ) Snapchat_cust_traffic_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ConversionRate_Adwords' KPI,
date, XTD,
ifnull(safe_divide(Adwords_cust_orders,Adwords_cust_traffic),0) value,
ifnull(safe_divide(Adwords_cust_orders_prevX,Adwords_cust_traffic_prevX),0) prevvalue,
ifnull(safe_divide(Adwords_cust_orders_PrevyearsameX,Adwords_cust_traffic_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Adwords_cust_orders' then Value else null end ) Adwords_cust_orders,
           max(case when KPI='Adwords_cust_orders' then prevValue else null end ) Adwords_cust_orders_PrevX,
           max(case when KPI='Adwords_cust_orders' then prevyearvalue else null end ) Adwords_cust_orders_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Adwords_cust_traffic' then Value else null end ) Adwords_cust_traffic,
           max(case when KPI='Adwords_cust_traffic' then prevValue else null end ) Adwords_cust_traffic_PrevX,
           max(case when KPI='Adwords_cust_traffic' then prevyearvalue else null end ) Adwords_cust_traffic_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ConversionRate_roi_hunter' KPI,
date, XTD,
ifnull(safe_divide(Roi_hunter_cust_orders,Roi_hunter_cust_traffic),0) value,
ifnull(safe_divide(Roi_hunter_cust_orders_prevX,Roi_hunter_cust_traffic_prevX),0) prevvalue,
ifnull(safe_divide(Roi_hunter_cust_orders_PrevyearsameX,Roi_hunter_cust_traffic_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Roi_hunter_cust_orders' then Value else null end ) Roi_hunter_cust_orders,
           max(case when KPI='Roi_hunter_cust_orders' then prevValue else null end ) Roi_hunter_cust_orders_PrevX,
           max(case when KPI='Roi_hunter_cust_orders' then prevyearvalue else null end ) Roi_hunter_cust_orders_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Roi_hunter_cust_traffic' then Value else null end ) Roi_hunter_cust_traffic,
           max(case when KPI='Roi_hunter_cust_traffic' then prevValue else null end ) Roi_hunter_cust_traffic_PrevX,
           max(case when KPI='Roi_hunter_cust_traffic' then prevyearvalue else null end ) Roi_hunter_cust_traffic_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Instagram_Unpaid_sessions_per' KPI,
date, XTD,
ifnull(safe_divide(Instagram_Unpaid_sessions,sessions),0) value,
ifnull(safe_divide(Instagram_Unpaid_sessions_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(Instagram_Unpaid_sessions_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Instagram_Unpaid_sessions' then Value else null end ) Instagram_Unpaid_sessions,
           max(case when KPI='Instagram_Unpaid_sessions' then prevValue else null end ) Instagram_Unpaid_sessions_PrevX,
           max(case when KPI='Instagram_Unpaid_sessions' then prevyearvalue else null end ) Instagram_Unpaid_sessions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'TikTok_paid_sessions_per' KPI,
date, XTD,
ifnull(safe_divide(TikTok_paid_sessions,sessions),0) value,
ifnull(safe_divide(TikTok_paid_sessions_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(TikTok_paid_sessions_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='TikTok_paid_sessions' then Value else null end ) TikTok_paid_sessions,
           max(case when KPI='TikTok_paid_sessions' then prevValue else null end ) TikTok_paid_sessions_PrevX,
           max(case when KPI='TikTok_paid_sessions' then prevyearvalue else null end ) TikTok_paid_sessions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'TikTok_Unpaid_sessions_per' KPI,
date, XTD,
ifnull(safe_divide(TikTok_Unpaid_sessions,sessions),0) value,
ifnull(safe_divide(TikTok_Unpaid_sessions_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(TikTok_Unpaid_sessions_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='TikTok_Unpaid_sessions' then Value else null end ) TikTok_Unpaid_sessions,
           max(case when KPI='TikTok_Unpaid_sessions' then prevValue else null end ) TikTok_Unpaid_sessions_PrevX,
           max(case when KPI='TikTok_Unpaid_sessions' then prevyearvalue else null end ) TikTok_Unpaid_sessions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Other_Social_sessions_per' KPI,
date, XTD,
ifnull(safe_divide(Other_Social_sessions,sessions),0) value,
ifnull(safe_divide(Other_Social_sessions_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(Other_Social_sessions_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Other_Social_sessions' then Value else null end ) Other_Social_sessions,
           max(case when KPI='Other_Social_sessions' then prevValue else null end ) Other_Social_sessions_PrevX,
           max(case when KPI='Other_Social_sessions' then prevyearvalue else null end ) Other_Social_sessions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Paid_Search_sessions_per' KPI,
date, XTD,
ifnull(safe_divide(Paid_Search_sessions,sessions),0) value,
ifnull(safe_divide(Paid_Search_sessions_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(Paid_Search_sessions_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Paid_Search_sessions' then Value else null end ) Paid_Search_sessions,
           max(case when KPI='Paid_Search_sessions' then prevValue else null end ) Paid_Search_sessions_PrevX,
           max(case when KPI='Paid_Search_sessions' then prevyearvalue else null end ) Paid_Search_sessions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Internal_Referral_sessions_per' KPI,
date, XTD,
ifnull(safe_divide(Internal_Referral_sessions,sessions),0) value,
ifnull(safe_divide(Internal_Referral_sessions_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(Internal_Referral_sessions_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Internal_Referral_sessions' then Value else null end ) Internal_Referral_sessions,
           max(case when KPI='Internal_Referral_sessions' then prevValue else null end ) Internal_Referral_sessions_PrevX,
           max(case when KPI='Internal_Referral_sessions' then prevyearvalue else null end ) Internal_Referral_sessions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Email_sessions_per' KPI,
date, XTD,
ifnull(safe_divide(Email_sessions,sessions),0) value,
ifnull(safe_divide(Email_sessions_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(Email_sessions_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Email_sessions' then Value else null end ) Email_sessions,
           max(case when KPI='Email_sessions' then prevValue else null end ) Email_sessions_PrevX,
           max(case when KPI='Email_sessions' then prevyearvalue else null end ) Email_sessions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Organic_Search_Sessions_per' KPI,
date, XTD,
ifnull(safe_divide(Organic_Search_Sessions,sessions),0) value,
ifnull(safe_divide(Organic_Search_Sessions_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(Organic_Search_Sessions_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Organic_Search_Sessions' then Value else null end ) Organic_Search_Sessions,
           max(case when KPI='Organic_Search_Sessions' then prevValue else null end ) Organic_Search_Sessions_PrevX,
           max(case when KPI='Organic_Search_Sessions' then prevyearvalue else null end ) Organic_Search_Sessions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Referral_Sessions_per' KPI,
date, XTD,
ifnull(safe_divide(Referral_Sessions,sessions),0) value,
ifnull(safe_divide(Referral_Sessions_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(Referral_Sessions_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Referral_Sessions' then Value else null end ) Referral_Sessions,
           max(case when KPI='Referral_Sessions' then prevValue else null end ) Referral_Sessions_PrevX,
           max(case when KPI='Referral_Sessions' then prevyearvalue else null end ) Referral_Sessions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Direct_Sessions_per' KPI,
date, XTD,
ifnull(safe_divide(Direct_Sessions,sessions),0) value,
ifnull(safe_divide(Direct_Sessions_prevX,sessions_prevX),0) prevvalue,
ifnull(safe_divide(Direct_Sessions_PrevyearsameX,sessions_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Direct_Sessions' then Value else null end ) Direct_Sessions,
           max(case when KPI='Direct_Sessions' then prevValue else null end ) Direct_Sessions_PrevX,
           max(case when KPI='Direct_Sessions' then prevyearvalue else null end ) Direct_Sessions_PrevyearsameX
       
                ,
        
        
           max(case when KPI='sessions' then Value else null end ) sessions,
           max(case when KPI='sessions' then prevValue else null end ) sessions_PrevX,
           max(case when KPI='sessions' then prevyearvalue else null end ) sessions_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'TikTok_paid_newusers_per' KPI,
date, XTD,
ifnull(safe_divide(TikTok_paid_newusers,newuserssourcemedium),0) value,
ifnull(safe_divide(TikTok_paid_newusers_prevX,newuserssourcemedium_prevX),0) prevvalue,
ifnull(safe_divide(TikTok_paid_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='TikTok_paid_newusers' then Value else null end ) TikTok_paid_newusers,
           max(case when KPI='TikTok_paid_newusers' then prevValue else null end ) TikTok_paid_newusers_PrevX,
           max(case when KPI='TikTok_paid_newusers' then prevyearvalue else null end ) TikTok_paid_newusers_PrevyearsameX
       
                ,
        
        
           max(case when KPI='newuserssourcemedium' then Value else null end ) newuserssourcemedium,
           max(case when KPI='newuserssourcemedium' then prevValue else null end ) newuserssourcemedium_PrevX,
           max(case when KPI='newuserssourcemedium' then prevyearvalue else null end ) newuserssourcemedium_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'TikTok_Unpaid_newusers_per' KPI,
date, XTD,
ifnull(safe_divide(TikTok_Unpaid_newusers,newuserssourcemedium),0) value,
ifnull(safe_divide(TikTok_Unpaid_newusers_prevX,newuserssourcemedium_prevX),0) prevvalue,
ifnull(safe_divide(TikTok_Unpaid_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='TikTok_Unpaid_newusers' then Value else null end ) TikTok_Unpaid_newusers,
           max(case when KPI='TikTok_Unpaid_newusers' then prevValue else null end ) TikTok_Unpaid_newusers_PrevX,
           max(case when KPI='TikTok_Unpaid_newusers' then prevyearvalue else null end ) TikTok_Unpaid_newusers_PrevyearsameX
       
                ,
        
        
           max(case when KPI='newuserssourcemedium' then Value else null end ) newuserssourcemedium,
           max(case when KPI='newuserssourcemedium' then prevValue else null end ) newuserssourcemedium_PrevX,
           max(case when KPI='newuserssourcemedium' then prevyearvalue else null end ) newuserssourcemedium_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Other_Social_newusers_per' KPI,
date, XTD,
ifnull(safe_divide(Other_Social_newusers,newuserssourcemedium),0) value,
ifnull(safe_divide(Other_Social_newusers_prevX,newuserssourcemedium_prevX),0) prevvalue,
ifnull(safe_divide(Other_Social_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Other_Social_newusers' then Value else null end ) Other_Social_newusers,
           max(case when KPI='Other_Social_newusers' then prevValue else null end ) Other_Social_newusers_PrevX,
           max(case when KPI='Other_Social_newusers' then prevyearvalue else null end ) Other_Social_newusers_PrevyearsameX
       
                ,
        
        
           max(case when KPI='newuserssourcemedium' then Value else null end ) newuserssourcemedium,
           max(case when KPI='newuserssourcemedium' then prevValue else null end ) newuserssourcemedium_PrevX,
           max(case when KPI='newuserssourcemedium' then prevyearvalue else null end ) newuserssourcemedium_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Paid_Search_newusers_per' KPI,
date, XTD,
ifnull(safe_divide(Paid_Search_newusers,newuserssourcemedium),0) value,
ifnull(safe_divide(Paid_Search_newusers_prevX,newuserssourcemedium_prevX),0) prevvalue,
ifnull(safe_divide(Paid_Search_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Paid_Search_newusers' then Value else null end ) Paid_Search_newusers,
           max(case when KPI='Paid_Search_newusers' then prevValue else null end ) Paid_Search_newusers_PrevX,
           max(case when KPI='Paid_Search_newusers' then prevyearvalue else null end ) Paid_Search_newusers_PrevyearsameX
       
                ,
        
        
           max(case when KPI='newuserssourcemedium' then Value else null end ) newuserssourcemedium,
           max(case when KPI='newuserssourcemedium' then prevValue else null end ) newuserssourcemedium_PrevX,
           max(case when KPI='newuserssourcemedium' then prevyearvalue else null end ) newuserssourcemedium_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Internal_Referral_newusers_per' KPI,
date, XTD,
ifnull(safe_divide(Internal_Referral_newusers,newuserssourcemedium),0) value,
ifnull(safe_divide(Internal_Referral_newusers_prevX,newuserssourcemedium_prevX),0) prevvalue,
ifnull(safe_divide(Internal_Referral_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Internal_Referral_newusers' then Value else null end ) Internal_Referral_newusers,
           max(case when KPI='Internal_Referral_newusers' then prevValue else null end ) Internal_Referral_newusers_PrevX,
           max(case when KPI='Internal_Referral_newusers' then prevyearvalue else null end ) Internal_Referral_newusers_PrevyearsameX
       
                ,
        
        
           max(case when KPI='newuserssourcemedium' then Value else null end ) newuserssourcemedium,
           max(case when KPI='newuserssourcemedium' then prevValue else null end ) newuserssourcemedium_PrevX,
           max(case when KPI='newuserssourcemedium' then prevyearvalue else null end ) newuserssourcemedium_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Email_newusers_per' KPI,
date, XTD,
ifnull(safe_divide(Email_newusers,newuserssourcemedium),0) value,
ifnull(safe_divide(Email_newusers_prevX,newuserssourcemedium_prevX),0) prevvalue,
ifnull(safe_divide(Email_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Email_newusers' then Value else null end ) Email_newusers,
           max(case when KPI='Email_newusers' then prevValue else null end ) Email_newusers_PrevX,
           max(case when KPI='Email_newusers' then prevyearvalue else null end ) Email_newusers_PrevyearsameX
       
                ,
        
        
           max(case when KPI='newuserssourcemedium' then Value else null end ) newuserssourcemedium,
           max(case when KPI='newuserssourcemedium' then prevValue else null end ) newuserssourcemedium_PrevX,
           max(case when KPI='newuserssourcemedium' then prevyearvalue else null end ) newuserssourcemedium_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Organic_Search_newusers_per' KPI,
date, XTD,
ifnull(safe_divide(Organic_Search_newusers,newuserssourcemedium),0) value,
ifnull(safe_divide(Organic_Search_newusers_prevX,newuserssourcemedium_prevX),0) prevvalue,
ifnull(safe_divide(Organic_Search_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Organic_Search_newusers' then Value else null end ) Organic_Search_newusers,
           max(case when KPI='Organic_Search_newusers' then prevValue else null end ) Organic_Search_newusers_PrevX,
           max(case when KPI='Organic_Search_newusers' then prevyearvalue else null end ) Organic_Search_newusers_PrevyearsameX
       
                ,
        
        
           max(case when KPI='newuserssourcemedium' then Value else null end ) newuserssourcemedium,
           max(case when KPI='newuserssourcemedium' then prevValue else null end ) newuserssourcemedium_PrevX,
           max(case when KPI='newuserssourcemedium' then prevyearvalue else null end ) newuserssourcemedium_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Referral_newusers_per' KPI,
date, XTD,
ifnull(safe_divide(Referral_newusers,newuserssourcemedium),0) value,
ifnull(safe_divide(Referral_newusers_prevX,newuserssourcemedium_prevX),0) prevvalue,
ifnull(safe_divide(Referral_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Referral_newusers' then Value else null end ) Referral_newusers,
           max(case when KPI='Referral_newusers' then prevValue else null end ) Referral_newusers_PrevX,
           max(case when KPI='Referral_newusers' then prevyearvalue else null end ) Referral_newusers_PrevyearsameX
       
                ,
        
        
           max(case when KPI='newuserssourcemedium' then Value else null end ) newuserssourcemedium,
           max(case when KPI='newuserssourcemedium' then prevValue else null end ) newuserssourcemedium_PrevX,
           max(case when KPI='newuserssourcemedium' then prevyearvalue else null end ) newuserssourcemedium_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Direct_newusers_per' KPI,
date, XTD,
ifnull(safe_divide(Direct_newusers,newuserssourcemedium),0) value,
ifnull(safe_divide(Direct_newusers_prevX,newuserssourcemedium_prevX),0) prevvalue,
ifnull(safe_divide(Direct_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Direct_newusers' then Value else null end ) Direct_newusers,
           max(case when KPI='Direct_newusers' then prevValue else null end ) Direct_newusers_PrevX,
           max(case when KPI='Direct_newusers' then prevyearvalue else null end ) Direct_newusers_PrevyearsameX
       
                ,
        
        
           max(case when KPI='newuserssourcemedium' then Value else null end ) newuserssourcemedium,
           max(case when KPI='newuserssourcemedium' then prevValue else null end ) newuserssourcemedium_PrevX,
           max(case when KPI='newuserssourcemedium' then prevyearvalue else null end ) newuserssourcemedium_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'CostperorderRand10_per' KPI,
date, XTD,
ifnull(safe_divide(Rand10_spend_day,Rand10_orders),0) value,
ifnull(safe_divide(Rand10_spend_day_prevX,Rand10_orders_prevX),0) prevvalue,
ifnull(safe_divide(Rand10_spend_day_PrevyearsameX,Rand10_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Rand10_spend_day' then Value else null end ) Rand10_spend_day,
           max(case when KPI='Rand10_spend_day' then prevValue else null end ) Rand10_spend_day_PrevX,
           max(case when KPI='Rand10_spend_day' then prevyearvalue else null end ) Rand10_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Rand10_orders' then Value else null end ) Rand10_orders,
           max(case when KPI='Rand10_orders' then prevValue else null end ) Rand10_orders_PrevX,
           max(case when KPI='Rand10_orders' then prevyearvalue else null end ) Rand10_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'CostperorderSatha10_per' KPI,
date, XTD,
ifnull(safe_divide(Satha10_spend_day,Satha10_orders),0) value,
ifnull(safe_divide(Satha10_spend_day_prevX,Satha10_orders_prevX),0) prevvalue,
ifnull(safe_divide(Satha10_spend_day_PrevyearsameX,Satha10_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Satha10_spend_day' then Value else null end ) Satha10_spend_day,
           max(case when KPI='Satha10_spend_day' then prevValue else null end ) Satha10_spend_day_PrevX,
           max(case when KPI='Satha10_spend_day' then prevyearvalue else null end ) Satha10_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Satha10_orders' then Value else null end ) Satha10_orders,
           max(case when KPI='Satha10_orders' then prevValue else null end ) Satha10_orders_PrevX,
           max(case when KPI='Satha10_orders' then prevyearvalue else null end ) Satha10_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'CostperorderWish10_per' KPI,
date, XTD,
ifnull(safe_divide(Wish10_spend_day,Wish10_orders),0) value,
ifnull(safe_divide(Wish10_spend_day_prevX,Wish10_orders_prevX),0) prevvalue,
ifnull(safe_divide(Wish10_spend_day_PrevyearsameX,Wish10_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Wish10_spend_day' then Value else null end ) Wish10_spend_day,
           max(case when KPI='Wish10_spend_day' then prevValue else null end ) Wish10_spend_day_PrevX,
           max(case when KPI='Wish10_spend_day' then prevyearvalue else null end ) Wish10_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Wish10_orders' then Value else null end ) Wish10_orders,
           max(case when KPI='Wish10_orders' then prevValue else null end ) Wish10_orders_PrevX,
           max(case when KPI='Wish10_orders' then prevyearvalue else null end ) Wish10_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ATV Customer Acq Rand10' KPI,
date, XTD,
ifnull(safe_divide(Rand10_revenue,Rand10_orders),0) value,
ifnull(safe_divide(Rand10_revenue_prevX,Rand10_orders_prevX),0) prevvalue,
ifnull(safe_divide(Rand10_revenue_PrevyearsameX,Rand10_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Rand10_revenue' then Value else null end ) Rand10_revenue,
           max(case when KPI='Rand10_revenue' then prevValue else null end ) Rand10_revenue_PrevX,
           max(case when KPI='Rand10_revenue' then prevyearvalue else null end ) Rand10_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Rand10_orders' then Value else null end ) Rand10_orders,
           max(case when KPI='Rand10_orders' then prevValue else null end ) Rand10_orders_PrevX,
           max(case when KPI='Rand10_orders' then prevyearvalue else null end ) Rand10_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ATV Customer Acq Satha10' KPI,
date, XTD,
ifnull(safe_divide(Satha10_revenue,Satha10_orders),0) value,
ifnull(safe_divide(Satha10_revenue_prevX,Satha10_orders_prevX),0) prevvalue,
ifnull(safe_divide(Satha10_revenue_PrevyearsameX,Satha10_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Satha10_revenue' then Value else null end ) Satha10_revenue,
           max(case when KPI='Satha10_revenue' then prevValue else null end ) Satha10_revenue_PrevX,
           max(case when KPI='Satha10_revenue' then prevyearvalue else null end ) Satha10_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Satha10_orders' then Value else null end ) Satha10_orders,
           max(case when KPI='Satha10_orders' then prevValue else null end ) Satha10_orders_PrevX,
           max(case when KPI='Satha10_orders' then prevyearvalue else null end ) Satha10_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ATV Customer Acq Wish10' KPI,
date, XTD,
ifnull(safe_divide(Wish10_revenue,Wish10_orders),0) value,
ifnull(safe_divide(Wish10_revenue_prevX,Wish10_orders_prevX),0) prevvalue,
ifnull(safe_divide(Wish10_revenue_PrevyearsameX,Wish10_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Wish10_revenue' then Value else null end ) Wish10_revenue,
           max(case when KPI='Wish10_revenue' then prevValue else null end ) Wish10_revenue_PrevX,
           max(case when KPI='Wish10_revenue' then prevyearvalue else null end ) Wish10_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Wish10_orders' then Value else null end ) Wish10_orders,
           max(case when KPI='Wish10_orders' then prevValue else null end ) Wish10_orders_PrevX,
           max(case when KPI='Wish10_orders' then prevyearvalue else null end ) Wish10_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Shareofsales_per Rand10' KPI,
date, XTD,
ifnull(safe_divide(Rand10_revenue,total_item_price_auth),0) value,
ifnull(safe_divide(Rand10_revenue_prevX,total_item_price_auth_prevX),0) prevvalue,
ifnull(safe_divide(Rand10_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Rand10_revenue' then Value else null end ) Rand10_revenue,
           max(case when KPI='Rand10_revenue' then prevValue else null end ) Rand10_revenue_PrevX,
           max(case when KPI='Rand10_revenue' then prevyearvalue else null end ) Rand10_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Shareofsales_per Satha10' KPI,
date, XTD,
ifnull(safe_divide(Satha10_revenue,total_item_price_auth),0) value,
ifnull(safe_divide(Satha10_revenue_prevX,total_item_price_auth_prevX),0) prevvalue,
ifnull(safe_divide(Satha10_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Satha10_revenue' then Value else null end ) Satha10_revenue,
           max(case when KPI='Satha10_revenue' then prevValue else null end ) Satha10_revenue_PrevX,
           max(case when KPI='Satha10_revenue' then prevyearvalue else null end ) Satha10_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Shareofsales_per Wish10' KPI,
date, XTD,
ifnull(safe_divide(Wish10_revenue,total_item_price_auth),0) value,
ifnull(safe_divide(Wish10_revenue_prevX,total_item_price_auth_prevX),0) prevvalue,
ifnull(safe_divide(Wish10_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Wish10_revenue' then Value else null end ) Wish10_revenue,
           max(case when KPI='Wish10_revenue' then prevValue else null end ) Wish10_revenue_PrevX,
           max(case when KPI='Wish10_revenue' then prevyearvalue else null end ) Wish10_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'confirmation_rate' KPI,
date, XTD,
ifnull(safe_divide(orders_confirmed,orders_created),0) value,
ifnull(safe_divide(orders_confirmed_prevX,orders_created_prevX),0) prevvalue,
ifnull(safe_divide(orders_confirmed_PrevyearsameX,orders_created_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='orders_confirmed' then Value else null end ) orders_confirmed,
           max(case when KPI='orders_confirmed' then prevValue else null end ) orders_confirmed_PrevX,
           max(case when KPI='orders_confirmed' then prevyearvalue else null end ) orders_confirmed_PrevyearsameX
       
                ,
        
        
           max(case when KPI='orders_created' then Value else null end ) orders_created,
           max(case when KPI='orders_created' then prevValue else null end ) orders_created_PrevX,
           max(case when KPI='orders_created' then prevyearvalue else null end ) orders_created_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'shipment_rate' KPI,
date, XTD,
ifnull(safe_divide(orders_shipped,orders_confirmed),0) value,
ifnull(safe_divide(orders_shipped_prevX,orders_confirmed_prevX),0) prevvalue,
ifnull(safe_divide(orders_shipped_PrevyearsameX,orders_confirmed_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='orders_shipped' then Value else null end ) orders_shipped,
           max(case when KPI='orders_shipped' then prevValue else null end ) orders_shipped_PrevX,
           max(case when KPI='orders_shipped' then prevyearvalue else null end ) orders_shipped_PrevyearsameX
       
                ,
        
        
           max(case when KPI='orders_confirmed' then Value else null end ) orders_confirmed,
           max(case when KPI='orders_confirmed' then prevValue else null end ) orders_confirmed_PrevX,
           max(case when KPI='orders_confirmed' then prevyearvalue else null end ) orders_confirmed_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'delivery_rate' KPI,
date, XTD,
ifnull(safe_divide(orders_delivered,orders_shipped),0) value,
ifnull(safe_divide(orders_delivered_prevX,orders_shipped_prevX),0) prevvalue,
ifnull(safe_divide(orders_delivered_PrevyearsameX,orders_shipped_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='orders_delivered' then Value else null end ) orders_delivered,
           max(case when KPI='orders_delivered' then prevValue else null end ) orders_delivered_PrevX,
           max(case when KPI='orders_delivered' then prevyearvalue else null end ) orders_delivered_PrevyearsameX
       
                ,
        
        
           max(case when KPI='orders_shipped' then Value else null end ) orders_shipped,
           max(case when KPI='orders_shipped' then prevValue else null end ) orders_shipped_PrevX,
           max(case when KPI='orders_shipped' then prevyearvalue else null end ) orders_shipped_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'fulfilment_rate' KPI,
date, XTD,
ifnull(safe_divide(orders_delivered,orders_confirmed),0) value,
ifnull(safe_divide(orders_delivered_prevX,orders_confirmed_prevX),0) prevvalue,
ifnull(safe_divide(orders_delivered_PrevyearsameX,orders_confirmed_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='orders_delivered' then Value else null end ) orders_delivered,
           max(case when KPI='orders_delivered' then prevValue else null end ) orders_delivered_PrevX,
           max(case when KPI='orders_delivered' then prevyearvalue else null end ) orders_delivered_PrevyearsameX
       
                ,
        
        
           max(case when KPI='orders_confirmed' then Value else null end ) orders_confirmed,
           max(case when KPI='orders_confirmed' then prevValue else null end ) orders_confirmed_PrevX,
           max(case when KPI='orders_confirmed' then prevyearvalue else null end ) orders_confirmed_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'CostperorderYasmine10_per' KPI,
date, XTD,
ifnull(safe_divide(Yasmine10_spend_day,Yasmine10_orders),0) value,
ifnull(safe_divide(Yasmine10_spend_day_prevX,Yasmine10_orders_prevX),0) prevvalue,
ifnull(safe_divide(Yasmine10_spend_day_PrevyearsameX,Yasmine10_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Yasmine10_spend_day' then Value else null end ) Yasmine10_spend_day,
           max(case when KPI='Yasmine10_spend_day' then prevValue else null end ) Yasmine10_spend_day_PrevX,
           max(case when KPI='Yasmine10_spend_day' then prevyearvalue else null end ) Yasmine10_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Yasmine10_orders' then Value else null end ) Yasmine10_orders,
           max(case when KPI='Yasmine10_orders' then prevValue else null end ) Yasmine10_orders_PrevX,
           max(case when KPI='Yasmine10_orders' then prevyearvalue else null end ) Yasmine10_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ATV Customer Acq Yasmine10' KPI,
date, XTD,
ifnull(safe_divide(Yasmine10_revenue,Yasmine10_orders),0) value,
ifnull(safe_divide(Yasmine10_revenue_prevX,Yasmine10_orders_prevX),0) prevvalue,
ifnull(safe_divide(Yasmine10_revenue_PrevyearsameX,Yasmine10_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Yasmine10_revenue' then Value else null end ) Yasmine10_revenue,
           max(case when KPI='Yasmine10_revenue' then prevValue else null end ) Yasmine10_revenue_PrevX,
           max(case when KPI='Yasmine10_revenue' then prevyearvalue else null end ) Yasmine10_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Yasmine10_orders' then Value else null end ) Yasmine10_orders,
           max(case when KPI='Yasmine10_orders' then prevValue else null end ) Yasmine10_orders_PrevX,
           max(case when KPI='Yasmine10_orders' then prevyearvalue else null end ) Yasmine10_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Shareofsales_per Yasmine10' KPI,
date, XTD,
ifnull(safe_divide(Yasmine10_revenue,total_item_price_auth),0) value,
ifnull(safe_divide(Yasmine10_revenue_prevX,total_item_price_auth_prevX),0) prevvalue,
ifnull(safe_divide(Yasmine10_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Yasmine10_revenue' then Value else null end ) Yasmine10_revenue,
           max(case when KPI='Yasmine10_revenue' then prevValue else null end ) Yasmine10_revenue_PrevX,
           max(case when KPI='Yasmine10_revenue' then prevyearvalue else null end ) Yasmine10_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'CostperorderFatma10_per' KPI,
date, XTD,
ifnull(safe_divide(Fatma10_spend_day,Fatma10_orders),0) value,
ifnull(safe_divide(Fatma10_spend_day_prevX,Fatma10_orders_prevX),0) prevvalue,
ifnull(safe_divide(Fatma10_spend_day_PrevyearsameX,Fatma10_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Fatma10_spend_day' then Value else null end ) Fatma10_spend_day,
           max(case when KPI='Fatma10_spend_day' then prevValue else null end ) Fatma10_spend_day_PrevX,
           max(case when KPI='Fatma10_spend_day' then prevyearvalue else null end ) Fatma10_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Fatma10_orders' then Value else null end ) Fatma10_orders,
           max(case when KPI='Fatma10_orders' then prevValue else null end ) Fatma10_orders_PrevX,
           max(case when KPI='Fatma10_orders' then prevyearvalue else null end ) Fatma10_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ATV Customer Acq Fatma10' KPI,
date, XTD,
ifnull(safe_divide(Fatma10_revenue,Fatma10_orders),0) value,
ifnull(safe_divide(Fatma10_revenue_prevX,Fatma10_orders_prevX),0) prevvalue,
ifnull(safe_divide(Fatma10_revenue_PrevyearsameX,Fatma10_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Fatma10_revenue' then Value else null end ) Fatma10_revenue,
           max(case when KPI='Fatma10_revenue' then prevValue else null end ) Fatma10_revenue_PrevX,
           max(case when KPI='Fatma10_revenue' then prevyearvalue else null end ) Fatma10_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Fatma10_orders' then Value else null end ) Fatma10_orders,
           max(case when KPI='Fatma10_orders' then prevValue else null end ) Fatma10_orders_PrevX,
           max(case when KPI='Fatma10_orders' then prevyearvalue else null end ) Fatma10_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Shareofsales_per Fatma10' KPI,
date, XTD,
ifnull(safe_divide(Fatma10_revenue,total_item_price_auth),0) value,
ifnull(safe_divide(Fatma10_revenue_prevX,total_item_price_auth_prevX),0) prevvalue,
ifnull(safe_divide(Fatma10_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Fatma10_revenue' then Value else null end ) Fatma10_revenue,
           max(case when KPI='Fatma10_revenue' then prevValue else null end ) Fatma10_revenue_PrevX,
           max(case when KPI='Fatma10_revenue' then prevyearvalue else null end ) Fatma10_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'total_days_to_close_per' KPI,
date, XTD,
ifnull(safe_divide(total_days_to_close,closed_tickets),0) value,
ifnull(safe_divide(total_days_to_close_prevX,closed_tickets_prevX),0) prevvalue,
ifnull(safe_divide(total_days_to_close_PrevyearsameX,closed_tickets_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='total_days_to_close' then Value else null end ) total_days_to_close,
           max(case when KPI='total_days_to_close' then prevValue else null end ) total_days_to_close_PrevX,
           max(case when KPI='total_days_to_close' then prevyearvalue else null end ) total_days_to_close_PrevyearsameX
       
                ,
        
        
           max(case when KPI='closed_tickets' then Value else null end ) closed_tickets,
           max(case when KPI='closed_tickets' then prevValue else null end ) closed_tickets_PrevX,
           max(case when KPI='closed_tickets' then prevyearvalue else null end ) closed_tickets_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'total_time_to_confirm_per' KPI,
date, XTD,
ifnull(safe_divide(total_time_to_confirm,orders_confirmed),0) value,
ifnull(safe_divide(total_time_to_confirm_prevX,orders_confirmed_prevX),0) prevvalue,
ifnull(safe_divide(total_time_to_confirm_PrevyearsameX,orders_confirmed_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='total_time_to_confirm' then Value else null end ) total_time_to_confirm,
           max(case when KPI='total_time_to_confirm' then prevValue else null end ) total_time_to_confirm_PrevX,
           max(case when KPI='total_time_to_confirm' then prevyearvalue else null end ) total_time_to_confirm_PrevyearsameX
       
                ,
        
        
           max(case when KPI='orders_confirmed' then Value else null end ) orders_confirmed,
           max(case when KPI='orders_confirmed' then prevValue else null end ) orders_confirmed_PrevX,
           max(case when KPI='orders_confirmed' then prevyearvalue else null end ) orders_confirmed_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'total_time_to_ship_per' KPI,
date, XTD,
ifnull(safe_divide(total_time_to_ship,orders_shipped),0) value,
ifnull(safe_divide(total_time_to_ship_prevX,orders_shipped_prevX),0) prevvalue,
ifnull(safe_divide(total_time_to_ship_PrevyearsameX,orders_shipped_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='total_time_to_ship' then Value else null end ) total_time_to_ship,
           max(case when KPI='total_time_to_ship' then prevValue else null end ) total_time_to_ship_PrevX,
           max(case when KPI='total_time_to_ship' then prevyearvalue else null end ) total_time_to_ship_PrevyearsameX
       
                ,
        
        
           max(case when KPI='orders_shipped' then Value else null end ) orders_shipped,
           max(case when KPI='orders_shipped' then prevValue else null end ) orders_shipped_PrevX,
           max(case when KPI='orders_shipped' then prevyearvalue else null end ) orders_shipped_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'total_time_to_deliver_per' KPI,
date, XTD,
ifnull(safe_divide(total_time_to_deliver,orders_delivered),0) value,
ifnull(safe_divide(total_time_to_deliver_prevX,orders_delivered_prevX),0) prevvalue,
ifnull(safe_divide(total_time_to_deliver_PrevyearsameX,orders_delivered_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='total_time_to_deliver' then Value else null end ) total_time_to_deliver,
           max(case when KPI='total_time_to_deliver' then prevValue else null end ) total_time_to_deliver_PrevX,
           max(case when KPI='total_time_to_deliver' then prevyearvalue else null end ) total_time_to_deliver_PrevyearsameX
       
                ,
        
        
           max(case when KPI='orders_delivered' then Value else null end ) orders_delivered,
           max(case when KPI='orders_delivered' then prevValue else null end ) orders_delivered_PrevX,
           max(case when KPI='orders_delivered' then prevyearvalue else null end ) orders_delivered_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'total_time_to_fulfill_per' KPI,
date, XTD,
ifnull(safe_divide(total_time_to_fulfill,orders_delivered),0) value,
ifnull(safe_divide(total_time_to_fulfill_prevX,orders_delivered_prevX),0) prevvalue,
ifnull(safe_divide(total_time_to_fulfill_PrevyearsameX,orders_delivered_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='total_time_to_fulfill' then Value else null end ) total_time_to_fulfill,
           max(case when KPI='total_time_to_fulfill' then prevValue else null end ) total_time_to_fulfill_PrevX,
           max(case when KPI='total_time_to_fulfill' then prevyearvalue else null end ) total_time_to_fulfill_PrevyearsameX
       
                ,
        
        
           max(case when KPI='orders_delivered' then Value else null end ) orders_delivered,
           max(case when KPI='orders_delivered' then prevValue else null end ) orders_delivered_PrevX,
           max(case when KPI='orders_delivered' then prevyearvalue else null end ) orders_delivered_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'total_attempts_per' KPI,
date, XTD,
ifnull(safe_divide(total_attempts,shipments),0) value,
ifnull(safe_divide(total_attempts_prevX,shipments_prevX),0) prevvalue,
ifnull(safe_divide(total_attempts_PrevyearsameX,shipments_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='total_attempts' then Value else null end ) total_attempts,
           max(case when KPI='total_attempts' then prevValue else null end ) total_attempts_PrevX,
           max(case when KPI='total_attempts' then prevyearvalue else null end ) total_attempts_PrevyearsameX
       
                ,
        
        
           max(case when KPI='shipments' then Value else null end ) shipments,
           max(case when KPI='shipments' then prevValue else null end ) shipments_PrevX,
           max(case when KPI='shipments' then prevyearvalue else null end ) shipments_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Costperordermissl2u_per' KPI,
date, XTD,
ifnull(safe_divide(missl2u_spend_day,missl2u_orders),0) value,
ifnull(safe_divide(missl2u_spend_day_prevX,missl2u_orders_prevX),0) prevvalue,
ifnull(safe_divide(missl2u_spend_day_PrevyearsameX,missl2u_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='missl2u_spend_day' then Value else null end ) missl2u_spend_day,
           max(case when KPI='missl2u_spend_day' then prevValue else null end ) missl2u_spend_day_PrevX,
           max(case when KPI='missl2u_spend_day' then prevyearvalue else null end ) missl2u_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='missl2u_orders' then Value else null end ) missl2u_orders,
           max(case when KPI='missl2u_orders' then prevValue else null end ) missl2u_orders_PrevX,
           max(case when KPI='missl2u_orders' then prevyearvalue else null end ) missl2u_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ATV Customer Acq missl2u' KPI,
date, XTD,
ifnull(safe_divide(missl2u_revenue,missl2u_orders),0) value,
ifnull(safe_divide(missl2u_revenue_prevX,missl2u_orders_prevX),0) prevvalue,
ifnull(safe_divide(missl2u_revenue_PrevyearsameX,missl2u_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='missl2u_revenue' then Value else null end ) missl2u_revenue,
           max(case when KPI='missl2u_revenue' then prevValue else null end ) missl2u_revenue_PrevX,
           max(case when KPI='missl2u_revenue' then prevyearvalue else null end ) missl2u_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='missl2u_orders' then Value else null end ) missl2u_orders,
           max(case when KPI='missl2u_orders' then prevValue else null end ) missl2u_orders_PrevX,
           max(case when KPI='missl2u_orders' then prevyearvalue else null end ) missl2u_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Shareofsales_per missl2u' KPI,
date, XTD,
ifnull(safe_divide(missl2u_revenue,total_item_price_auth),0) value,
ifnull(safe_divide(missl2u_revenue_prevX,total_item_price_auth_prevX),0) prevvalue,
ifnull(safe_divide(missl2u_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='missl2u_revenue' then Value else null end ) missl2u_revenue,
           max(case when KPI='missl2u_revenue' then prevValue else null end ) missl2u_revenue_PrevX,
           max(case when KPI='missl2u_revenue' then prevyearvalue else null end ) missl2u_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'CAC Total' KPI,
date, XTD,
ifnull(safe_divide((snapchat_cust_spend+spend+MissL10_spend_day+Husin10_spend_day+Feda10_spend_day+abrar21_spend_day+feda21_spend_day+roh_n2_spend_day+omseba10_spend_day+marwa10_spend_day+maha_140_spend_day+shahad10_spend_day+Rand10_spend_day+Satha10_spend_day+Wish10_spend_day+Yasmine10_spend_day+Fatma10_spend_day+missl2u_spend_day),New_customers),0) value,
ifnull(safe_divide((snapchat_cust_spend_prevX+spend_prevX+MissL10_spend_day_prevX+Husin10_spend_day_prevX+Feda10_spend_day_prevX+abrar21_spend_day_prevX+feda21_spend_day_prevX+roh_n2_spend_day_prevX+omseba10_spend_day_prevX+marwa10_spend_day_prevX+maha_140_spend_day_prevX+shahad10_spend_day_prevX+Rand10_spend_day_prevX+Satha10_spend_day_prevX+Wish10_spend_day_prevX+Yasmine10_spend_day_prevX+Fatma10_spend_day_prevX+missl2u_spend_day),New_customers),0) prevvalue,
ifnull(safe_divide((snapchat_cust_spend_PrevyearsameX+spend_PrevyearsameX+MissL10_spend_day_PrevyearsameX+Husin10_spend_day_PrevyearsameX+Feda10_spend_day_PrevyearsameX+abrar21_spend_day_PrevyearsameX+feda21_spend_day_PrevyearsameX+roh_n2_spend_day_PrevyearsameX+omseba10_spend_day_PrevyearsameX+marwa10_spend_day_PrevyearsameX+maha_140_spend_day_PrevyearsameX+shahad10_spend_day_PrevyearsameX+Rand10_spend_day_PrevyearsameX+Satha10_spend_day_PrevyearsameX+Wish10_spend_day_PrevyearsameX+Yasmine10_spend_day_PrevyearsameX+Fatma10_spend_day_PrevyearsameX+missl2u_spend_day),New_customers),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='snapchat_cust_spend' then Value else null end ) snapchat_cust_spend,
           max(case when KPI='snapchat_cust_spend' then prevValue else null end ) snapchat_cust_spend_PrevX,
           max(case when KPI='snapchat_cust_spend' then prevyearvalue else null end ) snapchat_cust_spend_PrevyearsameX
       
                ,
        
        
           max(case when KPI='spend' then Value else null end ) spend,
           max(case when KPI='spend' then prevValue else null end ) spend_PrevX,
           max(case when KPI='spend' then prevyearvalue else null end ) spend_PrevyearsameX
       
                ,
        
        
           max(case when KPI='MissL10_spend_day' then Value else null end ) MissL10_spend_day,
           max(case when KPI='MissL10_spend_day' then prevValue else null end ) MissL10_spend_day_PrevX,
           max(case when KPI='MissL10_spend_day' then prevyearvalue else null end ) MissL10_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Husin10_spend_day' then Value else null end ) Husin10_spend_day,
           max(case when KPI='Husin10_spend_day' then prevValue else null end ) Husin10_spend_day_PrevX,
           max(case when KPI='Husin10_spend_day' then prevyearvalue else null end ) Husin10_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Feda10_spend_day' then Value else null end ) Feda10_spend_day,
           max(case when KPI='Feda10_spend_day' then prevValue else null end ) Feda10_spend_day_PrevX,
           max(case when KPI='Feda10_spend_day' then prevyearvalue else null end ) Feda10_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='abrar21_spend_day' then Value else null end ) abrar21_spend_day,
           max(case when KPI='abrar21_spend_day' then prevValue else null end ) abrar21_spend_day_PrevX,
           max(case when KPI='abrar21_spend_day' then prevyearvalue else null end ) abrar21_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='feda21_spend_day' then Value else null end ) feda21_spend_day,
           max(case when KPI='feda21_spend_day' then prevValue else null end ) feda21_spend_day_PrevX,
           max(case when KPI='feda21_spend_day' then prevyearvalue else null end ) feda21_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='roh_n2_spend_day' then Value else null end ) roh_n2_spend_day,
           max(case when KPI='roh_n2_spend_day' then prevValue else null end ) roh_n2_spend_day_PrevX,
           max(case when KPI='roh_n2_spend_day' then prevyearvalue else null end ) roh_n2_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='omseba10_spend_day' then Value else null end ) omseba10_spend_day,
           max(case when KPI='omseba10_spend_day' then prevValue else null end ) omseba10_spend_day_PrevX,
           max(case when KPI='omseba10_spend_day' then prevyearvalue else null end ) omseba10_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='marwa10_spend_day' then Value else null end ) marwa10_spend_day,
           max(case when KPI='marwa10_spend_day' then prevValue else null end ) marwa10_spend_day_PrevX,
           max(case when KPI='marwa10_spend_day' then prevyearvalue else null end ) marwa10_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='maha_140_spend_day' then Value else null end ) maha_140_spend_day,
           max(case when KPI='maha_140_spend_day' then prevValue else null end ) maha_140_spend_day_PrevX,
           max(case when KPI='maha_140_spend_day' then prevyearvalue else null end ) maha_140_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='shahad10_spend_day' then Value else null end ) shahad10_spend_day,
           max(case when KPI='shahad10_spend_day' then prevValue else null end ) shahad10_spend_day_PrevX,
           max(case when KPI='shahad10_spend_day' then prevyearvalue else null end ) shahad10_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Rand10_spend_day' then Value else null end ) Rand10_spend_day,
           max(case when KPI='Rand10_spend_day' then prevValue else null end ) Rand10_spend_day_PrevX,
           max(case when KPI='Rand10_spend_day' then prevyearvalue else null end ) Rand10_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Satha10_spend_day' then Value else null end ) Satha10_spend_day,
           max(case when KPI='Satha10_spend_day' then prevValue else null end ) Satha10_spend_day_PrevX,
           max(case when KPI='Satha10_spend_day' then prevyearvalue else null end ) Satha10_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Wish10_spend_day' then Value else null end ) Wish10_spend_day,
           max(case when KPI='Wish10_spend_day' then prevValue else null end ) Wish10_spend_day_PrevX,
           max(case when KPI='Wish10_spend_day' then prevyearvalue else null end ) Wish10_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Yasmine10_spend_day' then Value else null end ) Yasmine10_spend_day,
           max(case when KPI='Yasmine10_spend_day' then prevValue else null end ) Yasmine10_spend_day_PrevX,
           max(case when KPI='Yasmine10_spend_day' then prevyearvalue else null end ) Yasmine10_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Fatma10_spend_day' then Value else null end ) Fatma10_spend_day,
           max(case when KPI='Fatma10_spend_day' then prevValue else null end ) Fatma10_spend_day_PrevX,
           max(case when KPI='Fatma10_spend_day' then prevyearvalue else null end ) Fatma10_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='missl2u_spend_day' then Value else null end ) missl2u_spend_day,
           max(case when KPI='missl2u_spend_day' then prevValue else null end ) missl2u_spend_day_PrevX,
           max(case when KPI='missl2u_spend_day' then prevyearvalue else null end ) missl2u_spend_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='New_customers' then Value else null end ) New_customers,
           max(case when KPI='New_customers' then prevValue else null end ) New_customers_PrevX,
           max(case when KPI='New_customers' then prevyearvalue else null end ) New_customers_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Costperorder_youtube' KPI,
date, XTD,
ifnull(safe_divide(spend_per_day,Youtube_cust_orders),0) value,
ifnull(safe_divide(spend_per_day_prevX,Youtube_cust_orders_prevX),0) prevvalue,
ifnull(safe_divide(spend_per_day_PrevyearsameX,Youtube_cust_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='spend_per_day' then Value else null end ) spend_per_day,
           max(case when KPI='spend_per_day' then prevValue else null end ) spend_per_day_PrevX,
           max(case when KPI='spend_per_day' then prevyearvalue else null end ) spend_per_day_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Youtube_cust_orders' then Value else null end ) Youtube_cust_orders,
           max(case when KPI='Youtube_cust_orders' then prevValue else null end ) Youtube_cust_orders_PrevX,
           max(case when KPI='Youtube_cust_orders' then prevyearvalue else null end ) Youtube_cust_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'AOV_youtube' KPI,
date, XTD,
ifnull(safe_divide(Youtube_cust_revenue,Youtube_cust_orders),0) value,
ifnull(safe_divide(Youtube_cust_revenue_prevX,Youtube_cust_orders_prevX),0) prevvalue,
ifnull(safe_divide(Youtube_cust_revenue_PrevyearsameX,Youtube_cust_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Youtube_cust_revenue' then Value else null end ) Youtube_cust_revenue,
           max(case when KPI='Youtube_cust_revenue' then prevValue else null end ) Youtube_cust_revenue_PrevX,
           max(case when KPI='Youtube_cust_revenue' then prevyearvalue else null end ) Youtube_cust_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Youtube_cust_orders' then Value else null end ) Youtube_cust_orders,
           max(case when KPI='Youtube_cust_orders' then prevValue else null end ) Youtube_cust_orders_PrevX,
           max(case when KPI='Youtube_cust_orders' then prevyearvalue else null end ) Youtube_cust_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'AOV_tiktok' KPI,
date, XTD,
ifnull(safe_divide(Tiktok_cust_revenue,Tiktok_cust_orders),0) value,
ifnull(safe_divide(Tiktok_cust_revenue_prevX,Tiktok_cust_orders_prevX),0) prevvalue,
ifnull(safe_divide(Tiktok_cust_revenue_PrevyearsameX,Tiktok_cust_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Tiktok_cust_revenue' then Value else null end ) Tiktok_cust_revenue,
           max(case when KPI='Tiktok_cust_revenue' then prevValue else null end ) Tiktok_cust_revenue_PrevX,
           max(case when KPI='Tiktok_cust_revenue' then prevyearvalue else null end ) Tiktok_cust_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Tiktok_cust_orders' then Value else null end ) Tiktok_cust_orders,
           max(case when KPI='Tiktok_cust_orders' then prevValue else null end ) Tiktok_cust_orders_PrevX,
           max(case when KPI='Tiktok_cust_orders' then prevyearvalue else null end ) Tiktok_cust_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Shareofsales_youtube' KPI,
date, XTD,
ifnull(safe_divide(Youtube_cust_revenue,total_item_price_auth),0) value,
ifnull(safe_divide(Youtube_cust_revenue_prevX,total_item_price_auth_prevX),0) prevvalue,
ifnull(safe_divide(Youtube_cust_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Youtube_cust_revenue' then Value else null end ) Youtube_cust_revenue,
           max(case when KPI='Youtube_cust_revenue' then prevValue else null end ) Youtube_cust_revenue_PrevX,
           max(case when KPI='Youtube_cust_revenue' then prevyearvalue else null end ) Youtube_cust_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Shareofsales_tiktok' KPI,
date, XTD,
ifnull(safe_divide(Tiktok_cust_revenue,total_item_price_auth),0) value,
ifnull(safe_divide(Tiktok_cust_revenue_prevX,total_item_price_auth_prevX),0) prevvalue,
ifnull(safe_divide(Tiktok_cust_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Tiktok_cust_revenue' then Value else null end ) Tiktok_cust_revenue,
           max(case when KPI='Tiktok_cust_revenue' then prevValue else null end ) Tiktok_cust_revenue_PrevX,
           max(case when KPI='Tiktok_cust_revenue' then prevyearvalue else null end ) Tiktok_cust_revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ConversionRate_youtube' KPI,
date, XTD,
ifnull(safe_divide(Youtube_cust_orders,Youtube_cust_traffic),0) value,
ifnull(safe_divide(Youtube_cust_orders_prevX,Youtube_cust_traffic_prevX),0) prevvalue,
ifnull(safe_divide(Youtube_cust_orders_PrevyearsameX,Youtube_cust_traffic_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Youtube_cust_orders' then Value else null end ) Youtube_cust_orders,
           max(case when KPI='Youtube_cust_orders' then prevValue else null end ) Youtube_cust_orders_PrevX,
           max(case when KPI='Youtube_cust_orders' then prevyearvalue else null end ) Youtube_cust_orders_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Youtube_cust_traffic' then Value else null end ) Youtube_cust_traffic,
           max(case when KPI='Youtube_cust_traffic' then prevValue else null end ) Youtube_cust_traffic_PrevX,
           max(case when KPI='Youtube_cust_traffic' then prevyearvalue else null end ) Youtube_cust_traffic_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ConversionRate_tiktok' KPI,
date, XTD,
ifnull(safe_divide(Tiktok_cust_orders,Tiktok_cust_traffic),0) value,
ifnull(safe_divide(Tiktok_cust_orders_prevX,Tiktok_cust_traffic_prevX),0) prevvalue,
ifnull(safe_divide(Tiktok_cust_orders_PrevyearsameX,Tiktok_cust_traffic_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Tiktok_cust_orders' then Value else null end ) Tiktok_cust_orders,
           max(case when KPI='Tiktok_cust_orders' then prevValue else null end ) Tiktok_cust_orders_PrevX,
           max(case when KPI='Tiktok_cust_orders' then prevyearvalue else null end ) Tiktok_cust_orders_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Tiktok_cust_traffic' then Value else null end ) Tiktok_cust_traffic,
           max(case when KPI='Tiktok_cust_traffic' then prevValue else null end ) Tiktok_cust_traffic_PrevX,
           max(case when KPI='Tiktok_cust_traffic' then prevyearvalue else null end ) Tiktok_cust_traffic_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'skus_80_20_per' KPI,
date, XTD,
ifnull(safe_divide(skus_80_20,num_of_skus),0) value,
ifnull(safe_divide(skus_80_20_prevX,num_of_skus_prevX),0) prevvalue,
ifnull(safe_divide(skus_80_20_PrevyearsameX,num_of_skus_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='skus_80_20' then Value else null end ) skus_80_20,
           max(case when KPI='skus_80_20' then prevValue else null end ) skus_80_20_PrevX,
           max(case when KPI='skus_80_20' then prevyearvalue else null end ) skus_80_20_PrevyearsameX
       
                ,
        
        
           max(case when KPI='num_of_skus' then Value else null end ) num_of_skus,
           max(case when KPI='num_of_skus' then prevValue else null end ) num_of_skus_PrevX,
           max(case when KPI='num_of_skus' then prevyearvalue else null end ) num_of_skus_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'New_customer_Revenue_per' KPI,
date, XTD,
ifnull(safe_divide(New_customer_Revenue,total_item_price_auth),0) value,
ifnull(safe_divide(New_customer_Revenue_prevX,total_item_price_auth_prevX),0) prevvalue,
ifnull(safe_divide(New_customer_Revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='New_customer_Revenue' then Value else null end ) New_customer_Revenue,
           max(case when KPI='New_customer_Revenue' then prevValue else null end ) New_customer_Revenue_PrevX,
           max(case when KPI='New_customer_Revenue' then prevyearvalue else null end ) New_customer_Revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Existing_customer_Revenue_per' KPI,
date, XTD,
ifnull(safe_divide(Existing_customer_Revenue,total_item_price_auth),0) value,
ifnull(safe_divide(Existing_customer_Revenue_prevX,total_item_price_auth_prevX),0) prevvalue,
ifnull(safe_divide(Existing_customer_Revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='Existing_customer_Revenue' then Value else null end ) Existing_customer_Revenue,
           max(case when KPI='Existing_customer_Revenue' then prevValue else null end ) Existing_customer_Revenue_PrevX,
           max(case when KPI='Existing_customer_Revenue' then prevyearvalue else null end ) Existing_customer_Revenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='total_item_price_auth' then Value else null end ) total_item_price_auth,
           max(case when KPI='total_item_price_auth' then prevValue else null end ) total_item_price_auth_PrevX,
           max(case when KPI='total_item_price_auth' then prevyearvalue else null end ) total_item_price_auth_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'Costperorder_Tiktok' KPI,
date, XTD,
ifnull(safe_divide(tiktok_cust_spend,Tiktok_cust_orders),0) value,
ifnull(safe_divide(tiktok_cust_spend_prevX,Tiktok_cust_orders_prevX),0) prevvalue,
ifnull(safe_divide(tiktok_cust_spend_PrevyearsameX,Tiktok_cust_orders_PrevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='tiktok_cust_spend' then Value else null end ) tiktok_cust_spend,
           max(case when KPI='tiktok_cust_spend' then prevValue else null end ) tiktok_cust_spend_PrevX,
           max(case when KPI='tiktok_cust_spend' then prevyearvalue else null end ) tiktok_cust_spend_PrevyearsameX
       
                ,
        
        
           max(case when KPI='Tiktok_cust_orders' then Value else null end ) Tiktok_cust_orders,
           max(case when KPI='Tiktok_cust_orders' then prevValue else null end ) Tiktok_cust_orders_PrevX,
           max(case when KPI='Tiktok_cust_orders' then prevyearvalue else null end ) Tiktok_cust_orders_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ROAS_1' KPI,
date, XTD,
ifnull(safe_divide(NetRevenue,spend),0) value,
ifnull(safe_divide(NetRevenue_prevX,spend_prevX),0) prevvalue,
ifnull(safe_divide(NetRevenue_prevyearsameX,spend_prevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='NetRevenue' then Value else null end ) NetRevenue,
           max(case when KPI='NetRevenue' then prevValue else null end ) NetRevenue_PrevX,
           max(case when KPI='NetRevenue' then prevyearvalue else null end ) NetRevenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='spend' then Value else null end ) spend,
           max(case when KPI='spend' then prevValue else null end ) spend_PrevX,
           max(case when KPI='spend' then prevyearvalue else null end ) spend_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       
                union all
        

select
Halo_country,
'ROAS_2' KPI,
date, XTD,
ifnull(safe_divide(NetRevenue,spend),0) value,
ifnull(safe_divide(NetRevenue_prevX,spend_prevX),0) prevvalue,
ifnull(safe_divide(NetRevenue_prevyearsameX,spend_prevyearsameX),0) prevyearvalue,
Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate
from(
select Halo_country,date,XTD,Startdate,Enddate,PrevX_Startdate,PrevX_Enddate,PrevYearX_Startdate,PrevYearX_Enddate,
       
           max(case when KPI='NetRevenue' then Value else null end ) NetRevenue,
           max(case when KPI='NetRevenue' then prevValue else null end ) NetRevenue_PrevX,
           max(case when KPI='NetRevenue' then prevyearvalue else null end ) NetRevenue_PrevyearsameX
       
                ,
        
        
           max(case when KPI='spend' then Value else null end ) spend,
           max(case when KPI='spend' then prevValue else null end ) spend_PrevX,
           max(case when KPI='spend' then prevyearvalue else null end ) spend_PrevyearsameX
       
        
from `noted-computing-279322.halo_1_1.fKPIs`
group by 1,2,3,4,5,6,7,8,9
)
       


) ta,
`noted-computing-279322.halo_1_1.refKPI` tb
where ta.KPI = tb.KPI_VARIABLE
union all
select ta.*,
concat(format_date('%d %b %y',parse_date('%Y-%m-%d',Startdate)),' - ',format_date('%d %b %y',parse_date('%Y-%m-%d',Enddate))) Value_DateRange,
concat(format_date('%d %b %y',parse_date('%Y-%m-%d',PrevX_Startdate)),' - ',format_date('%d %b %y',parse_date('%Y-%m-%d',PrevX_Enddate))) PrevValue_DateRange,
concat(format_date('%d %b %y',parse_date('%Y-%m-%d',PrevYearX_Startdate)),' - ',format_date('%d %b %y',parse_date('%Y-%m-%d',PrevYearX_Enddate))) PrevYearValue_DateRange,
case when lower(KPI) like '%bounce%' and lower(KPI) not like '%non%bounce' then safe_divide(-(Value-prevValue),prevValue) else safe_divide((Value-prevValue),prevValue) end Change_PrevX,
case when lower(KPI) like '%bounce%' and lower(KPI) not like '%non%bounce' then safe_divide(-(Value-prevyearvalue),prevyearvalue) else safe_divide((Value-prevyearvalue),prevyearvalue) end Change_PrevYearX,
tb.KPI_ID,
tb.KPI_FORMAT
from
(
select
Halo_country,
KPI,
'WTD' date,
'Week' XTD,
sum(case when PARSE_DATE('%Y-%m-%d', date) between DATE_TRUNC(CURRENT_DATE(), ISOWEEK) and current_date() then Value else 0 end)Value,
sum(case when PARSE_DATE('%Y-%m-%d', date) between DATE_TRUNC(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), ISOWEEK), INTERVAL 1 DAY), ISOWEEK) and DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK) then Value else 0 end) prevValue,
sum(case when PARSE_DATE('%Y-%m-%d', date) between DATE_SUB(DATE_TRUNC(CURRENT_DATE(),ISOWEEK),INTERVAL 1 YEAR) and DATE_SUB(CURRENT_DATE(),INTERVAL 1 YEAR) then Value else 0 end) prevyearvalue,

min(case when PARSE_DATE('%Y-%m-%d', date) between DATE_TRUNC(CURRENT_DATE(), ISOWEEK) and current_date() then date else null end) startdate,
max(case when PARSE_DATE('%Y-%m-%d', date) between DATE_TRUNC(CURRENT_DATE(), ISOWEEK) and current_date() then date else '0' end) enddate,

min(case when PARSE_DATE('%Y-%m-%d', date) between DATE_TRUNC(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), ISOWEEK), INTERVAL 1 DAY), ISOWEEK) and DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK) then date else null end) PrevX_Startdate,
max(case when PARSE_DATE('%Y-%m-%d', date) between DATE_TRUNC(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), ISOWEEK), INTERVAL 1 DAY), ISOWEEK) and DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK) then date else '0' end) PrevX_Enddate,


min(case when PARSE_DATE('%Y-%m-%d', date) between DATE_SUB(DATE_TRUNC(CURRENT_DATE(),ISOWEEK),INTERVAL 1 YEAR) and DATE_SUB(CURRENT_DATE(),INTERVAL 1 YEAR) then date else null end) PrevYearX_Startdate,
max(case when PARSE_DATE('%Y-%m-%d', date) between DATE_SUB(DATE_TRUNC(CURRENT_DATE(),ISOWEEK),INTERVAL 1 YEAR) and DATE_SUB(CURRENT_DATE(),INTERVAL 1 YEAR) then date else '0' end) PrevYearX_Enddate



from `noted-computing-279322.halo_1_1.fKPIs`
where XTD = 'Day'
--and PARSE_DATE('%Y%m%d', date) between DATE_TRUNC(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK), INTERVAL 1 DAY), WEEK) and current_date()
group by Halo_country, KPI

union all

select
Halo_country,
KPI,
'MTD' date,
'Month' XTD,
sum(case when PARSE_DATE('%Y-%m-%d', date) between DATE_TRUNC(CURRENT_DATE(), MONTH) and current_date() then Value else 0 end) Value,
sum(case when PARSE_DATE('%Y-%m-%d', date) between DATE_TRUNC(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY), MONTH) and DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) then Value else 0 end) prevValue,
sum(case when PARSE_DATE('%Y-%m-%d', date) between DATE_SUB(DATE_TRUNC(CURRENT_DATE(),MONTH),INTERVAL 1 YEAR) and DATE_SUB(CURRENT_DATE(),INTERVAL 1 YEAR) then Value else 0 end) prevyearvalue,

min(case when PARSE_DATE('%Y-%m-%d', date) between DATE_TRUNC(CURRENT_DATE(), MONTH) and current_date() then date else null end) startdate,
max(case when PARSE_DATE('%Y-%m-%d', date) between DATE_TRUNC(CURRENT_DATE(), MONTH) and current_date() then date else '0' end) enddate,

min(case when PARSE_DATE('%Y-%m-%d', date) between DATE_TRUNC(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY), MONTH) and DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) then date else null end) PrevX_Startdate,
max(case when PARSE_DATE('%Y-%m-%d', date) between DATE_TRUNC(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY), MONTH) and DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) then date else '0' end) PrevX_Enddate,

min(case when PARSE_DATE('%Y-%m-%d', date) between DATE_SUB(DATE_TRUNC(CURRENT_DATE(),MONTH),INTERVAL 1 YEAR) and DATE_SUB(CURRENT_DATE(),INTERVAL 1 YEAR) then date else null end) PrevYearX_Startdate,
max(case when PARSE_DATE('%Y-%m-%d', date) between DATE_SUB(DATE_TRUNC(CURRENT_DATE(),MONTH),INTERVAL 1 YEAR) and DATE_SUB(CURRENT_DATE(),INTERVAL 1 YEAR) then date else '0' end) PrevYearX_Enddate

from `noted-computing-279322.halo_1_1.fKPIs`
where XTD = 'Day'
-- and PARSE_DATE('%Y%m%d', date) between DATE_TRUNC(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 2 DAY), MONTH) and current_date()
group by Halo_country, KPI

union all

select
Halo_country,
KPI,
'QTD' date,
'Quarter' XTD,
sum(case when PARSE_DATE('%Y-%m-%d', date) between DATE_TRUNC(CURRENT_DATE(), QUARTER) and current_date() then Value else 0 end) Value,
sum(case when PARSE_DATE('%Y-%m-%d', date) between DATE_TRUNC(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 1 DAY), QUARTER) and DATE_SUB(CURRENT_DATE(), INTERVAL 1 QUARTER) then Value else 0 end) prevValue,
sum(case when PARSE_DATE('%Y-%m-%d', date) between DATE_SUB(DATE_TRUNC(CURRENT_DATE(),QUARTER),INTERVAL 1 YEAR) and DATE_SUB(CURRENT_DATE(),INTERVAL 1 YEAR) then Value else 0 end) prevyearvalue,

min(case when PARSE_DATE('%Y-%m-%d', date) between DATE_TRUNC(CURRENT_DATE(), QUARTER) and current_date() then  date else null end) startdate,
max(case when PARSE_DATE('%Y-%m-%d', date) between DATE_TRUNC(CURRENT_DATE(), QUARTER) and current_date() then  date else '0' end) enddate,

min(case when PARSE_DATE('%Y-%m-%d', date) between DATE_TRUNC(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 1 DAY), QUARTER) and DATE_SUB(CURRENT_DATE(), INTERVAL 1 QUARTER) then date else null  end) PrevX_Startdate,
max(case when PARSE_DATE('%Y-%m-%d', date) between DATE_TRUNC(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 1 DAY), QUARTER) and DATE_SUB(CURRENT_DATE(), INTERVAL 1 QUARTER) then date else '0' end) PrevX_Enddate,

min(case when PARSE_DATE('%Y-%m-%d', date) between DATE_SUB(DATE_TRUNC(CURRENT_DATE(),QUARTER),INTERVAL 1 YEAR) and DATE_SUB(CURRENT_DATE(),INTERVAL 1 YEAR) then date else null end) PrevYearX_Startdate,
max(case when PARSE_DATE('%Y-%m-%d', date) between DATE_SUB(DATE_TRUNC(CURRENT_DATE(),QUARTER),INTERVAL 1 YEAR) and DATE_SUB(CURRENT_DATE(),INTERVAL 1 YEAR) then date else '0' end) PrevYearX_Enddate




from `noted-computing-279322.halo_1_1.fKPIs`
where XTD = 'Day'
--and PARSE_DATE('%Y%m%d', date) between DATE_TRUNC(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 1 DAY), QUARTER) and current_date()
group by Halo_country, KPI

union all

select
Halo_country,
KPI,
'YTD' date,
'Year' XTD,
sum(case when PARSE_DATE('%Y-%m-%d', date) between DATE_TRUNC(CURRENT_DATE(), YEAR) and current_date() then Value else 0 end) Value,
sum(case when PARSE_DATE('%Y-%m-%d', date) between DATE_TRUNC(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), YEAR), INTERVAL 1 DAY), YEAR) and DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR) then Value else 0 end) prevValue,
sum(case when PARSE_DATE('%Y-%m-%d', date) between DATE_SUB(DATE_TRUNC(CURRENT_DATE(),YEAR),INTERVAL 1 YEAR) and DATE_SUB(CURRENT_DATE(),INTERVAL 1 YEAR) then Value else 0 end) prevyearvalue,

min(case when PARSE_DATE('%Y-%m-%d', date) between DATE_TRUNC(CURRENT_DATE(), YEAR) and current_date() then date else null end) startdate,
max(case when PARSE_DATE('%Y-%m-%d', date) between DATE_TRUNC(CURRENT_DATE(), YEAR) and current_date() then date else '0' end) enddate,

min(case when PARSE_DATE('%Y-%m-%d', date) between DATE_TRUNC(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), YEAR), INTERVAL 1 DAY), YEAR) and DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR) then date else null end) PrevX_Startdate,
max(case when PARSE_DATE('%Y-%m-%d', date) between DATE_TRUNC(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), YEAR), INTERVAL 1 DAY), YEAR) and DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR) then date else '0' end) PrevX_Enddate,
min(case when PARSE_DATE('%Y-%m-%d', date) between DATE_SUB(DATE_TRUNC(CURRENT_DATE(),YEAR),INTERVAL 1 YEAR) and DATE_SUB(CURRENT_DATE(),INTERVAL 1 YEAR) then date else null end) PrevYearX_Startdate,
max(case when PARSE_DATE('%Y-%m-%d', date) between DATE_SUB(DATE_TRUNC(CURRENT_DATE(),YEAR),INTERVAL 1 YEAR) and DATE_SUB(CURRENT_DATE(),INTERVAL 1 YEAR) then date else '0' end) PrevYearX_Enddate


from `noted-computing-279322.halo_1_1.fKPIs`
where XTD = 'Day'
-- and PARSE_DATE('%Y-%m-%d', date) between DATE_TRUNC(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), YEAR), INTERVAL 1 DAY), YEAR) and current_date()
group by Halo_country, KPI
) ta,
`noted-computing-279322.halo_1_1.refKPI` tb
where ta.KPI = tb.KPI_VARIABLE
union all
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
) end ,0) prevyearvalue,

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
select Halo_country,
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
sum(Value) Value,
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
       
                STRUCT('Sessions_F' as Metric, Sessions_F as Data)
            
                ,
            
        
                STRUCT('Impressions_F' as Metric, Impressions_F as Data)
            
                ,
            
        
                STRUCT('Adclicks_F' as Metric, Adclicks_F as Data)
            
                ,
            
        
                STRUCT('Addtocarts_F' as Metric, Addtocarts_F as Data)
            
                ,
            
        
                STRUCT('Bounces_F' as Metric, Bounces_F as Data)
            
                ,
            
        
                STRUCT('Spend_F' as Metric, Spend_F as Data)
            
                ,
            
        
                STRUCT('Transactions_F' as Metric, Transactions_F as Data)
            
                ,
            
        
                STRUCT('Revenue_F' as Metric, Revenue_F as Data)
            
        
        ] Metrics_data
        from (
select  Halo_Country,
        date_start date,
        sum(sessions) Sessions_F,
        sum(Impressions) Impressions_F,
        sum(Adclickss) Adclicks_F,
        sum(Addtocarts) Addtocarts_F ,
        sum(Bounces) Bounces_F,
        sum(Spend) Spend_F,
        sum(Orders) Transactions_F,
        sum(Revenue) Revenue_F
       from `noted-computing-279322.halo_1_1.fDailyAggMarketingFBGoogle`
       where source_medium in ('facebook / cpc','facebook / paid')
--        and  date_start!=current_date()
       group by 1,2
   ))
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
union all
select ta.* ,
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

lag(Startdate,1) over (partition by Halo_country,KPI,period_flag order by period_id) PrevX_Startdate,

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
select Halo_country,
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
sum(Value) Value,
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
       
                STRUCT('Sessions_G' as Metric, Sessions_G as Data)
            
                ,
            
        
                STRUCT('Impressions_G' as Metric, Impressions_G as Data)
            
                ,
            
        
                STRUCT('Adclicks_G' as Metric, Adclicks_G as Data)
            
                ,
            
        
                STRUCT('Addtocarts_G' as Metric, Addtocarts_G as Data)
            
                ,
            
        
                STRUCT('Bounces_G' as Metric, Bounces_G as Data)
            
                ,
            
        
                STRUCT('Spend_G' as Metric, Spend_G as Data)
            
                ,
            
        
                STRUCT('Transactions_G' as Metric, Transactions_G as Data)
            
                ,
            
        
                STRUCT('Revenue_G' as Metric, Revenue_G as Data)
            
        
        ] Metrics_data
        from (
select Halo_Country,date_start date,sum(sessions) Sessions_G, sum(Impressions) Impressions_G, sum(Adclickss) Adclicks_G, sum(Addtocarts) Addtocarts_G , sum(Bounces) Bounces_G, sum(Spend) Spend_G,
       sum(Orders) Transactions_G,sum(Revenue) Revenue_G
       from `noted-computing-279322.halo_1_1.fDailyAggMarketingFBGoogle` where source_medium in ('google / cpc')
--                                                                  and  date_start!=current_date()
       group by 1,2

   ))
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

