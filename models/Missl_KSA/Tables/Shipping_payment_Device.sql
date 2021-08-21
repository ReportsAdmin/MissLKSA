select safe_divide(PaymentPage,Transaction) payment_trnx ,D_ga_deviceCategory,StartDate,'MissL' Halo_Country from(
select sum(case when  D_ga_shoppingStage = "TRANSACTION" then M_ga_sessions else 0 end) as Transaction,
sum(case when D_ga_shoppingStage = "CHECKOUT_5" then M_ga_sessions else 0 end) as PaymentPage,D_ga_deviceCategory,StartDate from (

select StartDate, D_ga_deviceCategory , D_ga_shoppingStage, cast(M_ga_sessions as int64) M_ga_sessions
from `noted-computing-279322.halo_1_1.fGABaseShoppingStages` )

group by 3,4)