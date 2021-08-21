select StartDate,D_ga_internalPromotionId, D_ga_internalPromotionName,
D_ga_internalPromotionPosition, M_ga_internalPromotionClicks, M_ga_internalPromotionViews from(
select * except(row_number) from(
select *,row_number() over (partition by StartDate,D_ga_internalPromotionId, D_ga_internalPromotionName,
D_ga_internalPromotionPosition, M_ga_internalPromotionClicks, M_ga_internalPromotionViews
order by _daton_batch_runtime desc) as row_number from `noted-computing-279322.MissL1.halo_ga1_216655333_banner`)
where row_number=1)