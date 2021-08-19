SELECT D_ga_landingPagePath,  Startdate,
sum(cast(M_ga_sessions as float64)) as sessions,
sum(cast( M_ga_transactions as float64)) as orders,
sum(cast( M_ga_transactionRevenue as float64)) as revenue,
avg(cast( M_ga_bounceRate as float64)) as bounce_rate,
avg(cast( M_ga_avgSessionDuration as float64)) as avgsessionduration,
FROM `noted-computing-279322.MissL1.MissL_LandingPages_216655333_Landingpage`
group by 1,2