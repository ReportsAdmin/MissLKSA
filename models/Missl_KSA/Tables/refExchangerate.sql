select Date, base_currency_code,order_currency_code,exchange_rate  from
(select distinct b.Date as Date, case when exchange_rate is null then 1 else exchange_rate end as  exchange_rate ,
case when order_currency_code is null then 'INR' else order_currency_code end as order_currency_code,
case when base_currency_code is null then  'USD' else base_currency_code end as base_currency_code ,
row_number() over(partition by date  order by date )ranki
from
(select *
from `noted-computing-279322.halo_1_1.Calendar`
order by 1 asc) b
left join

(select distinct base_to_order_rate exchange_rate,base_currency_code,order_currency_code, extract(date from created_at) ord_date
from `noted-computing-279322.halo_1_1.magento_transaction`
where created_at >= '2013-01-01' and order_currency_code = 'INR' order by ord_date ) a

on b.Date=a.ord_date)
where ranki = 1
order by 1 ASC