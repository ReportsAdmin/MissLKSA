select parse_date("%Y%m%d",D_ga_date) start_date,
a.D_ga_transactionId order_id,b.ad_cat_id,b.is_ad_order,b.Paid_NonPaid,D_ga_deviceCategory DeviceCategory,
'MissL' Halo_Country
from
    (
SELECT * except(row_number)
FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY D_ga_transactionId  order by startdate) row_number
FROM `noted-computing-279322.halo_1_1.fGABaseTransactions`)
WHERE row_number = 1
) a
, `noted-computing-279322.halo_1_1.refKeywords`  b
where a.D_ga_keyword=b.keyword
and a.D_ga_adContent=b.ad_content
and a.D_ga_campaign= b. campaign_name
and a.D_ga_sourceMedium=b.source_medium