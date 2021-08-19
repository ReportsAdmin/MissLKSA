
select date,code,spend,orders,revenue,ifnull(share_of_sales,0) share_of_sales,ifnull(cpo,0) cpo,ifnull(aov,0) aov
from
(
select *,round(100*safe_divide(revenue,total_revenue),2) share_of_sales,round(safe_divide(spend,orders),2) cpo,
round(safe_divide(revenue,orders),2) aov
from
(select x.date,x.code,x.spend,x.orders,round(x.revenue,2) revenue,round(y.total_item_price_auth,2) total_revenue
from
(
select date,code,
ifnull(spend,0) spend,
ifnull(orders,0) orders,
ifnull(revenue,0) revenue
from
(
select date,code,max(spend) spend,
count(distinct case when order_status = 'successful' then increment_id end) orders,
sum(case when order_status = 'successful' then total_item_price end) revenue
from
((select c.date,c.code,d.spend_per_day spend
from
(select distinct date,lower(trim(code)) code,monthname,a.year,country_ksa_egypt
from `noted-computing-279322.halo_1_1.Calendar` a
cross join
`noted-computing-279322.halo_1_1.magento_influencerscode` b) c
left join `noted-computing-279322.halo_1_1.magento_influencerscode` d
on c.code = lower(trim(d.code))
and lower(c.monthname) = lower(substr(d.month_paid,1,3))
and c.year = d.year
where lower(c.country_ksa_egypt) = 'ksa') e
left join `noted-computing-279322.halo_1_1.fOrders` f
on e.date = f.order_date
and e.code = lower(f.coupon_code))
group by 1,2) n
where orders > 0 or spend > 0) x
left join `noted-computing-279322.halo_1_1.fDailyAgg` y
on x.date = y.date))
