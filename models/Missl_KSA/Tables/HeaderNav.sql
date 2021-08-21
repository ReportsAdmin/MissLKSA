select StartDate,D_ga_eventCategory,D_ga_eventAction,D_ga_eventLabel,sum(cast(M_ga_uniqueEvents as int64)) H_Nav,'MissL' Halo_Country from
`noted-computing-279322.halo_1_1.fGABaseEvents`
  where lower(D_ga_eventCategory) like '%header%' and lower(D_ga_eventCategory) not like '%headernavigation%' 
group by 1,2,3,4,6
