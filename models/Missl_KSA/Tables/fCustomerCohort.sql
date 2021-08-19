select p.*,ifnull(q.Cust,0) Cust,ifnull(q.orders,0) orders,ifnull(q.Revenue,0) Revenue,ifnull(q.Value,0) Value,
       {{var('Country')}} Halo_Country
from
(select x.*,y.cust_total
from
(select a.AcquisitionDate,b.OrderDate,c.Type,split(AcquisitionDate ,',')[safe_ordinal(2)] AcquisitionYear,
        split(OrderDate ,',')[safe_ordinal(2)] OrderYear
from
(select *,row_number() over (order by year ,month) rank
from(
select distinct extract(month from order_date) month,extract(year from order_date) year,format_date("%B,%Y",order_date) AcquisitionDate
from {{var('fOrders')}}
)) a,
(select *,row_number() over (order by year ,month) rank
from(
select distinct extract(month from order_date) month,extract(year from order_date) year,format_date("%B,%Y",order_date) OrderDate
from {{var('fOrders')}}
))b,
(select 'Retention Rate' Type union all select 'Orders' union all select  'Revenue') c
where a.rank<=b.rank) x

left join

(select format_date("%B,%Y",order_date) AS AcquisitionDate,count(distinct lower(user_id)) cust_total
from {{var('fOrders')}}
where Is_Successful_Order=true and is_new_customer =1
group by 1) y

on x.AcquisitionDate=y.AcquisitionDate
) p

left join


(select *,{{var('Country')}} Halo_Country
from(
select a.*,b.Cust as cust_total, (a.Cust/b.Cust) as Value,'Retention Rate' Type
from
(
SELECT format_date("%B,%Y",customer_acq_date) AS AcquisitionDate,
  format_date("%Y",customer_acq_date) AS AcquisitionYear,
  format_date("%B,%Y",order_date) AS OrderDate,
  format_date("%Y", order_date) AS OrderYear,
  COUNT(distinct(lower(user_id))) as Cust,
  count(distinct(order_id) ) orders,
  sum(total_item_price) Revenue
  from {{var('fOrders')}} a
  Inner join
  (
    select (lower(user_id)) cus_id, min(order_date) customer_acq_date
    from {{var('fOrders')}} where Is_Successful_Order = true and  order_date!=current_date()
    group by cus_id
  )b
on (lower(a.user_id))= b.cus_id where a.Is_Successful_Order=true
GROUP BY 1,2,3,4
--having AcquisitionMonth is not null
ORDER BY 1,2,3,4
)a

left join

(
SELECT format_date("%B,%Y", customer_acq_date) AS AcquisitionDate,
  format_date("%Y", customer_acq_date) AS AcquisitionYear,
  ifnull(COUNT(distinct (lower(user_id))),0) as Cust
  from {{var('fOrders')}} a
  inner join
  (
    select (lower(user_id)) cus_id,min(order_date) customer_acq_date
     from {{var('fOrders')}} where Is_Successful_Order = true and  order_date!=current_date()
     group by cus_id
  )b
on (lower(a.user_id))= b.cus_id where a.Is_Successful_Order=true
GROUP BY 1,2
--having AcquisitionMonth is not null
ORDER BY 1,2
)b

on a.AcquisitionDate=b.AcquisitionDate --a.AcquisitionMonth = b.AcquisitionMonth
and a.AcquisitionYear = b.AcquisitionYear


union all



select a.*,b.Cust as cust_total, orders as Value,'Orders' Type
from
(
SELECT format_date("%B,%Y",customer_acq_date) AS AcquisitionDate,
  format_date("%Y",customer_acq_date) AS AcquisitionYear,
  format_date("%B,%Y",order_date) AS OrderDate,
  format_date("%Y", order_date) AS OrderYear,
  COUNT(distinct (lower(user_id))) as Cust,
  count(distinct(order_id) ) orders,
  sum(total_item_price) Revenue
  from {{var('fOrders')}} a
  Inner join
  (
    select (lower(user_id)) cus_id, min(order_date) customer_acq_date
    from {{var('fOrders')}} where Is_Successful_Order=true and  order_date!=current_date()
    group by cus_id
  )b
on lower((a.user_id))= b.cus_id where a.Is_Successful_Order=true
GROUP BY 1,2,3,4
--having AcquisitionMonth is not null
ORDER BY 1,2,3,4
)a

left join

(
SELECT format_date("%B,%Y", customer_acq_date) AS AcquisitionDate,
  format_date("%Y", customer_acq_date) AS AcquisitionYear,
  ifnull(COUNT(distinct (lower(user_id))),0) as Cust
  from {{var('fOrders')}} a
  inner join
  (
    select (lower(user_id)) cus_id,min(order_date) customer_acq_date
     from {{var('fOrders')}} where Is_Successful_Order=true and  order_date!=current_date()
     group by cus_id
  )b
on (lower(a.user_id))= b.cus_id where a.Is_Successful_Order=true
GROUP BY 1,2
--having AcquisitionMonth is not null
ORDER BY 1,2
)b

on a.AcquisitionDate=b.AcquisitionDate --a.AcquisitionMonth = b.AcquisitionMonth
and a.AcquisitionYear = b.AcquisitionYear


union all


select a.*,b.Cust as cust_total, Revenue as Value,'Revenue' Type
from
(
SELECT format_date("%B,%Y",customer_acq_date) AS AcquisitionDate,
  format_date("%Y",customer_acq_date) AS AcquisitionYear,
  format_date("%B,%Y",order_date) AS OrderDate,
  format_date("%Y", order_date) AS OrderYear,
  COUNT(distinct (lower(user_id))) as Cust,
  count(distinct(order_id) ) orders,
  sum(total_item_price) Revenue
  from {{var('fOrders')}} a
  Inner join
  (
    select (lower(user_id)) cus_id, min(order_date) customer_acq_date
    from {{var('fOrders')}} where Is_Successful_Order=true and  order_date!=current_date()
    group by cus_id
  )b
on (lower(a.user_id))= b.cus_id where a.Is_Successful_Order=true
GROUP BY 1,2,3,4
--having AcquisitionMonth is not null
ORDER BY 1,2,3,4
)a

left join

(
SELECT format_date("%B,%Y", customer_acq_date) AS AcquisitionDate,
  format_date("%Y", customer_acq_date) AS AcquisitionYear,
  COUNT(distinct (lower(user_id))) as Cust
  from {{var('fOrders')}} a
  inner join
  (
    select (lower(user_id)) cus_id,min(order_date) customer_acq_date
     from {{var('fOrders')}} where Is_Successful_Order=true and  order_date!=current_date()
     group by cus_id
  )b
on (lower(a.user_id))= b.cus_id where a.Is_Successful_Order=true
GROUP BY 1,2
--having AcquisitionMonth is not null
ORDER BY 1,2
)b

on a.AcquisitionDate=b.AcquisitionDate --a.AcquisitionMonth = b.AcquisitionMonth
and a.AcquisitionYear = b.AcquisitionYear

)) q

on p.AcquisitionDate=q.AcquisitionDate and p.OrderDate=q.OrderDate and p.Type=q.Type

order by AcquisitionDate


