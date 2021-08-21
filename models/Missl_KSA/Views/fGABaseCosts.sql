select  * from
(select *except(sm_old,sm_ga,sm_expectation) from
(select *except(campaign_name), ifnull(sm_expectation,ifnull(sm_old,'')) D_ga_sourcemedium from
(select distinct *except(d_ga_sourcemedium),d_ga_sourcemedium as sm_old from
(SELECT * except(row_number) FROM
(SELECT *,ROW_NUMBER() OVER (PARTITION BY D_ga_date,D_ga_sourceMedium,D_ga_campaign,D_ga_keyword,D_ga_adContent
                                               order by _daton_batch_runtime desc) row_number
FROM `noted-computing-279322.MissL1.halo_ga1_216655333_cost`
)

WHERE row_number = 1))a
left join
(select distinct sm_ga,campaign_name,sm_expectation
from `noted-computing-279322.halo_1_1.channel_ksa`)b
on a.D_ga_campaign = b.campaign_name and a.sm_old = b.sm_ga)
)

