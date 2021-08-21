select parse_date("%Y%m%d",D_ga_date) date,D_ga_deviceCategory,D_ga_browser,cast(M_ga_sessions as int64) as Sessions,cast(M_ga_bounces as int64) as Bounce,(cast(M_ga_avgSessionDuration as float64)/60) AvgSessionDuration
,cast(M_ga_avgPageLoadTime  as float64) as avgpgloadtime,cast(M_ga_transactions as int64) transactions,
'MissL' Halo_Country
from `noted-computing-279322.halo_1_1.fGADeviceBrowser`