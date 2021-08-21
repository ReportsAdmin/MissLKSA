select row_number() over() disc_cat_id,disc_cat,'MissL' Halo_Country
from
(
select 'no_promo' disc_cat
union all select '0-30%'
union all select '30%-50%'
union all select '>50%'
)