
select a.product_id,a.date_start,a.date_stop,b.product_segment_id,'MissL' Halo_Country
from
(select product_id,case when Percentile_Orders =10 then 'Top Selling'
                       when Percentile_Impressions=2 and Percentile_CTR=1 and Percentile_CDR=1 and Percentile_BDR=1 then 'Unpopular'
                       when Percentile_Impressions=2 and Percentile_CTR=1 and Percentile_CDR=2 and Percentile_BDR=2 then 'Hidden Diamond'
                       when Percentile_Impressions=2 and Percentile_CTR=2 and Percentile_CDR=2 and Percentile_BDR=1 then 'High Priced'
                       when Percentile_Impressions=2 and Percentile_CTR=2 and Percentile_CDR=1 and Percentile_BDR=2 then 'Hidden Diamond'
                       when Percentile_Impressions=1 and Percentile_CTR=2 and Percentile_CDR=2 and Percentile_BDR=1 then 'High Priced'
                       when Percentile_Impressions=2 and Percentile_CTR=2 and Percentile_CDR=2 and Percentile_BDR=2 then 'Hidden Diamond'
                       when Percentile_Impressions=1 and Percentile_CTR=1 and Percentile_CDR=2 and Percentile_BDR=2 then 'Hidden Diamond'
                       when Percentile_Impressions=2 and Percentile_CTR=2 and Percentile_CDR=1 and Percentile_BDR=1 then 'High Priced'
                       when Percentile_Impressions=1 and Percentile_CTR=1 and Percentile_CDR=1 and Percentile_BDR=2 then 'Unpopular'
                       when Percentile_Impressions=1 and Percentile_CTR=2 and Percentile_CDR=2 and Percentile_BDR=2 then 'Hidden Diamond'
                       when Percentile_Impressions=1 and Percentile_CTR=1 and Percentile_CDR=2 and Percentile_BDR=1 then 'Unpopular'
                       when Percentile_Impressions=2 and Percentile_CTR=1 and Percentile_CDR=1 and Percentile_BDR=2 then 'High Priced'
                       when Percentile_Impressions=2 and Percentile_CTR=1 and Percentile_CDR=2 and Percentile_BDR=1 then 'Unpopular'
                       when Percentile_Impressions=1 and Percentile_CTR=2 and Percentile_CDR=1 and Percentile_BDR=2 then 'Hidden Diamond'
                       when Percentile_Impressions=1 and Percentile_CTR=2 and Percentile_CDR=1 and Percentile_BDR=1 then 'Unpopular'
                       else 'Unpopular' end Productsegments,date_start,date_stop
from
(
 
select  product_id,Impressions,ntile(2) over (order by Impressions asc) Percentile_Impressions,
                   CTR,ntile(2) over (order by CTR asc) Percentile_CTR,
                   CDR,ntile(2) over (order by CDR asc) Percentile_CDR,
                   BDR,ntile(2) over (order by BDR asc) Percentile_BDR,
                   Orders,ntile(10) over (order by orders) Percentile_Orders,
                   date_start,date_stop
from
(
select product_id, Impressions,safe_divide(clicks,impressions) CTR,
      safe_divide(Addstocart,Productdetail_views) CDR,safe_divide(orders,Productdetail_views) BDR,orders,date_start,date_stop
from
(
select product_id,sum(impressions) Impressions,sum(clicks) clicks,sum(productdetail_views) Productdetail_views,
       sum(Addstocart) Addstocart,sum(orders) orders,min(date_start) date_start,max(date_start) date_stop
from `noted-computing-279322.halo_1_1.fProductInsights`
group by product_id
)
--where impressions!=0 and Productdetail_views!=0
)
)) a,
`noted-computing-279322.halo_1_1.refProductSegments` b
where a.productsegments = b.product_segment