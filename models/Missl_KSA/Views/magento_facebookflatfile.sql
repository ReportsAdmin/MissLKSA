select * from
((select *,case when campaign_name like '%KSA%' then 'KSA'
when campaign_name like '%SA%' then 'KSA'
when campaign_name like '%Saudi%' then 'KSA' else 'Egypt' end as country,
case when campaign_name like '%BF%' then 'True' else 'False' end as Retargeting from
(select distinct date_start,campaign_id,campaign_name,publisher_platform as platform,sum(spend)cost
from `noted-computing-279322.MissL1.facebook_ads_380276816001252_adinsights_breakdown_publisher_platform`
where date_start <= '2020-08-31'
group by 1,2,3,4))
union all
(select *,'KSA' as country,
case when campaign_name like '%BF%' then 'True' else 'False' end as Retargeting from
(select distinct date_start,campaign_id,campaign_name,publisher_platform as platform,sum(spend)cost
from `noted-computing-279322.MissL1.Halo_ksa_fb_421543735473067_adinsights_breakdown_publisher_platform`
where date_start > '2020-08-31'
group by 1,2,3,4)))
where country = 'KSA'