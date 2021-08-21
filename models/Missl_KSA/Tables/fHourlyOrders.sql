select *,Round((PrevWeekOrders+Prev2WeekOrders+Prev3WeekOrders)/3) Prev_3Weeks_AVg_Orders,
        Round((PrevWeekRevenue+Prev2WeekRevenue+Prev3WeekRevenue)/3) Prev_3Weeks_AVg_Revenue,
        'MissL' Halo_Country
from
(
select *,ifnull(lag(Orders,7) over (partition by hour order by date),0) PrevWeekOrders,
         ifnull(lag(Revenue,7) over (partition by hour order by date),0) PrevWeekRevenue,
         ifnull(lag(Orders,14) over (partition by hour order by date),0) Prev2WeekOrders,
         ifnull(lag(Revenue,14) over (partition by hour order by date),0) Prev2WeekRevenue,
         ifnull(lag(Orders,21) over (partition by hour order by date),0) Prev3WeekOrders,
         ifnull(lag(Revenue,21) over (partition by hour order by date),0) Prev3WeekRevenue,
         ifnull(lag(Orders,1) over (partition by hour order by date),0) PrevDayOrders,
         ifnull(lag(Revenue,1) over (partition by hour order by date),0) PrevDayRevenue
from(
select p.*,ifnull(q.Orders,0) Orders,ifnull(q.Revenue,0) Revenue
from
(
(select a.date,b.hour
from
(select date from UNNEST (GENERATE_DATE_ARRAY('2018-01-01',CURRENT_DATE(), INTERVAL 1 DAY)) as Date) a,
(select hour from UNNEST (GENERATE_ARRAY(0,23,1)) as hour) b) p
left join
(select order_date,extract(hour from order_datetime) Hour,ifnull(count(distinct order_id),0) Orders,sum(Revenue) Revenue
from
(
select order_id,order_date,order_datetime,sum(total_item_price) Revenue
from `noted-computing-279322.halo_1_1.fOrders`
where Is_Successful_Order =true
group by 1,2,3
)
group by 1,2) q
on p.date=q.order_date and p.hour=q.hour
)
)
)
