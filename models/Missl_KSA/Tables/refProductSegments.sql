
select row_number() over() product_segment_id,product_segment,'MissL' Halo_Country
from
(
select 'Top Selling' product_segment
union all select 'Hidden Diamond'
union all select 'Unpopular'
union all select 'High Priced'
)