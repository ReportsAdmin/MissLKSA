select Date,
extract(month from Date) Month,
extract(isoweek from Date) week,
format_date('%b',date) Monthname,
extract(year from Date) Year
,'MissL' Halo_Country
from UNNEST (GENERATE_DATE_ARRAY('2020-01-01',CURRENT_DATE(), INTERVAL 1 DAY)) as Date