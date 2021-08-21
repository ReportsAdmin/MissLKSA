select * except(ranki) ,'MissL' Halo_Country
from
(select *,rank() over (partition by user_info order by userid desc) ranki from(
select activateddate,	user_info,	cast(userid as string) userid
from(

select distinct *
from
(
select ifnull(user.userid,guest.userid) userid,ifnull(user.email,guest.email) user_info,
        ifnull(user.activateddate,guest.activateddate) activateddate
from
(
(select entity_id userid,email,datetime(created_at,"Asia/Riyadh") activateddate from
`noted-computing-279322.halo_1_1.magento_tbluserinfo`)  user
full outer join
(select customer_id userid,lower(customer_email) email, min(datetime(created_at,"Asia/Riyadh")) activateddate
from  `noted-computing-279322.halo_1_1.magento_transaction`
group by customer_id,customer_email) guest
on lower(user.email)=lower(guest.email)
)
)
)
)) where ranki=1