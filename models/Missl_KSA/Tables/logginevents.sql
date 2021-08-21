select cal.date,a.FB_M_ga_uniqueEvents,G_M_ga_uniqueEvents,L_M_ga_uniqueEvents ,'MissL' Halo_Country from
(select Date from `noted-computing-279322.halo_1_1.Calendar` )cal
left join
(select distinct StartDate,sum(cast(M_ga_uniqueEvents as int64)) FB_M_ga_uniqueEvents
from `noted-computing-279322.halo_1_1.fGABaseEvents`
   where D_ga_eventAction  like '%You have successfully logged in using your Facebook account.%'
group by 1)a
on cal.date=a.StartDate

left join

(select distinct StartDate,sum(cast(M_ga_uniqueEvents as int64)) G_M_ga_uniqueEvents
from `noted-computing-279322.halo_1_1.fGABaseEvents`
   where D_ga_eventAction  like '%You have successfully logged in using your Google account.%'
group by 1)b

on cal.date=b.StartDate

left join

(select distinct StartDate,sum(cast(M_ga_uniqueEvents as int64)) L_M_ga_uniqueEvents
from `noted-computing-279322.halo_1_1.fGABaseEvents`
   where D_ga_eventAction like '%login%'
group by 1)c

on cal.Date=c.StartDate
order by L_M_ga_uniqueEvents desc
