select concat(cast(extract(year from order_date) as string),'-',cast(extract( month from order_date) as string)) as year_month,
case when is_new_customer=0 then 'returning_customer'
     else 'new_customer'
     end user_type,
     sum(total_item_price) as revenue,count(distinct(user_id)) as number,sum(product_quantity) as quantity_sold,count(distinct(increment_id)) as transactions, sum(total_item_price)/count(distinct(increment_id)) as average_transaction_value

from (Select order_date,is_new_customer,lower(user_id) user_id,total_item_price, product_quantity,increment_id
      from `{{fOrders}}`
       where order_status='successful'
       )
group by 2,1
order by 1,2