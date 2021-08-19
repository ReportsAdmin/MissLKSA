select * from
 (select * except(row_number) from(
select *,row_number() over ( partition by StartDate, D_ga_userAgeBracket order by _daton_batch_runtime desc) row_number
from {{var('ga_age')}}
)
where row_number=1)