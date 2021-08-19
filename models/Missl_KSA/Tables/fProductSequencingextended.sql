select *
from (select *,row_number() over (partition by category_L2 order by AverageRev desc) DesiredPositioning,{{Country}} Halo_Country
        from(select a.*,safe_divide(Revenue,30) AverageRev,activationdate.activation_date
                          from (select a.*,c.CatlogSequence
                                from (select distinct product_id,
                                        category_id,
                                        category_L2,
                                        product_sku,
                                        product_title prod_name,
                                        image,
                                        inventory,
                                        sum(impressions) impressions,
                                        sum(orders) orders,
                                        sum(units) units,
                                        sum(revenue) revenue
                                        from `{{fProductInsightsextended}}`
                                        where date_start between date_sub(current_date(),Interval 1 Month) and current_date()
                                        group by 1,2,3,4,5,6,7) a
                                  left  join (select *
                                              from (select distinct product_id,position CatlogSequence,
                                                    row_number() over (partition by product_id,category_id order by _daton_batch_runtime desc) rownumber,category_id
                                                    from `{{B_magento_productstatic}}`
                                                    )
                                              where rownumber=1) c on a.product_id=cast(c.product_id as string) and a.category_id = c.category_id
                                )a
                          left join (select distinct entity_id,min(date(created_at)) as Activation_date
                                      from `{{magento_productentity}}`
                                      group by 1)activationdate on a.product_id=cast(activationdate.entity_id as string)



                )
        )
