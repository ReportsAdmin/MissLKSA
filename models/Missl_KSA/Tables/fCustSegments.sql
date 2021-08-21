
select a.user_id,b.cust_segment_id,a.is_active,a.is_lapsed,a.is_churned,a.First_pur_date,a.last_pur_date,'MissL' Halo_Country
from
(select user_id,
            case
              when Percentile_PercentRevenue=10 and  Cust_type='RepeatPurchase' and DaysSincePurchase<=270 then 'High Value Active'
              when Percentile_PercentRevenue=10 and  Cust_type='RepeatPurchase' and DaysSincePurchase<=450 then 'High Value Lapsed'
              when Percentile_PercentRevenue=10 and  Cust_type='RepeatPurchase' and DaysSincePurchase>450 then 'High Value Churned'
              when Cust_type='RepeatPurchase' and DaysSincePurchase<=270 then 'Multiple Purchase Active'
              when Cust_type='RepeatPurchase' and DaysSincePurchase<=450 then 'Multiple Purchase Lapsed'
              when Cust_type='RepeatPurchase' and DaysSincePurchase>450 then 'Multiple Purchase Churned'
              when Cust_type='SinglePurchase' and DaysSincePurchase<=270 then 'Single Purchase Active'
              when Cust_type='SinglePurchase' and DaysSincePurchase<=450 then 'Single Purchase Lapsed'
              when Cust_type='SinglePurchase' and DaysSincePurchase>450 then 'Single Purchase Churned'
              end Cust_Segment,
              cast(case when DaysSincePurchase<=270 then 1 else 0 end as boolean) is_active,
              cast(case when DaysSincePurchase>270 and DaysSincePurchase<=450 then 1 else 0 end as boolean) is_lapsed,
              cast(case when DaysSincePurchase>450 then 1 else 0 end as boolean) is_churned,
              First_pur_date,last_pur_date
from
(           
select *,ntile(10) over (order by PercentRevenue) Percentile_PercentRevenue
from
(
select *,date_diff(current_date(),last_pur_date,day) DaysSincePurchase,(Revenue)/(sum(revenue) over ())*100 PercentRevenue,
       case when orders=1 then 'SinglePurchase'
       else 'RepeatPurchase' end Cust_type
from `noted-computing-279322.halo_1_1.fCustInsights`
order by Revenue desc
)
)) a,
`noted-computing-279322.halo_1_1.refCustSegments` b
where a.cust_segment=b.cust_segment
