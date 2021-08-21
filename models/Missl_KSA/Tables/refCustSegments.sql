select row_number() over() cust_segment_id,cust_segment,'MissL' Halo_Country
from
(
select 'High Value Active' cust_segment
union all select 'High Value Lapsed'
union all select 'High Value Churned'
union all select 'Single Purchase Active'
union all select 'Single Purchase Lapsed'
union all select 'Single Purchase Churned'
union all select 'Multiple Purchase Active'
union all select 'Multiple Purchase Lapsed'
union all select 'Multiple Purchase Churned'
)