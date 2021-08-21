select * from
(
select  * from (
SELECT * except(row_number)
FROM (
SELECT *, 'KSA' as country, ROW_NUMBER() OVER (partition by ad_id,date_start order by _daton_batch_runtime desc) row_number
FROM `noted-computing-279322.MissL1.Halo_ksa_fb_421543735473067_adinsights`)
WHERE row_number = 1
and date_start > '2020-08-31'
)
union all
select  * from (
SELECT * except(row_number)
FROM (
SELECT *, case when campaign_name like '%KSA%' then 'KSA'
when campaign_name like '%SA%' then 'KSA'
when campaign_name like '%Saudi%' then 'KSA' else 'Egypt' end as country,
ROW_NUMBER() OVER (partition by ad_id,date_start order by _daton_batch_runtime desc) row_number
FROM `noted-computing-279322.MissL1.halo_fb_380276816001252_adinsights`)
WHERE row_number = 1
and date_start <= '2020-08-31'
)
    )
where country  = 'KSA'