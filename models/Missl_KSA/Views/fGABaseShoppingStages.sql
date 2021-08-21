
(select *except(sm_old,sm_ga,sm_expectation) from
(select *except(campaign_name), ifnull(sm_expectation,ifnull(sm_old,'')) D_ga_sourcemedium from
(select distinct *except(d_ga_sourcemedium),d_ga_sourcemedium as sm_old from (
SELECT * except(row_number)
FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY D_ga_date,D_ga_sourceMedium,D_ga_campaign,D_ga_keyword,D_ga_adContent,
                                               D_ga_deviceCategory,D_ga_shoppingStage order by _daton_batch_runtime desc) row_number
FROM `noted-computing-279322.MissL1.halo_ga1_216655333_shoppingstages`
where D_ga_campaign in
(select distinct campaignname from `noted-computing-279322.MissL1.Missl_KSA_Google_Ads_5117479097_CAMPAIGN_PERFORMANCE_REPORT`)
and D_ga_sourceMedium in ('%google%')
)
WHERE row_number = 1
)) a
left join
(select distinct sm_ga,campaign_name,sm_expectation
from `noted-computing-279322.halo_1_1.channel_ksa`)b
on a.D_ga_campaign = b.campaign_name and a.sm_old = b.sm_ga)
)
--
union all

(select *except(sm_old,sm_ga,sm_expectation) from
(select *except(campaign_name), ifnull(sm_expectation,ifnull(sm_old,'')) D_ga_sourcemedium from
(select distinct *except(d_ga_sourcemedium),d_ga_sourcemedium as sm_old from (
SELECT * except(row_number)
FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY D_ga_date,D_ga_sourceMedium,D_ga_campaign,D_ga_keyword,D_ga_adContent,
                                               D_ga_deviceCategory,D_ga_shoppingStage order by _daton_batch_runtime desc) row_number
FROM `noted-computing-279322.MissL1.halo_ga1_216655333_shoppingstages`
where D_ga_sourceMedium not in ('%google%'))
WHERE row_number = 1
)) a
left join
(select distinct sm_ga,campaign_name,sm_expectation
from `noted-computing-279322.halo_1_1.channel_ksa`)b
on a.D_ga_campaign = b.campaign_name and a.sm_old = b.sm_ga)
)