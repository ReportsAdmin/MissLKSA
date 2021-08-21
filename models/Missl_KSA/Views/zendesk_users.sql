
select *except(rownumber) ,
case when Status ='closed' then Updated_date
else NULL
end as Closing_Date from
(select distinct c.created_date as Date, c.updated_date as Updated_date,
c.status as Status ,c.brand_id as Brand_ID,c.id as Ticket_ID,c.channel as Channel,c.requester_id as Requester_ID,d.name as Requester_Name,
row_number() over(partition by c.id order by c.updated_date desc) as rownumber,
c.tags as Tags, 'MissL' Halo_Country
from
(
select distinct date(datetime(cast(a.created_at as timestamp),'Asia/Riyadh'))created_date, date(datetime(cast(a.updated_at as timestamp),'Asia/Riyadh'))updated_date,  a.brand_id,
a.id,a.requester_id,b.channel,a.tags,a.status
from `noted-computing-279322.MissL1.zendesk_Tickets` a,unnest(via) b) c
left join `noted-computing-279322.MissL1.zendesk_Users` d
on c.requester_id = d.id
)
where rownumber=1