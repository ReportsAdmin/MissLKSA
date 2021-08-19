select dat.Ticket_Open, dat.Ticket_closed , tick.status, tick.channel, tick.Complaints  from (
(Select a.*, b.* from (
(Select distinct FORMAT_DATE("%b-%Y", Date) as Ticket_Open from {{var('zendesk_users')}}) a
join (Select distinct FORMAT_DATE("%b-%Y", Date) as Ticket_closed from {{var('zendesk_users')}}) b
on (a.Ticket_Open >= b.Ticket_closed) or (a.Ticket_Open < b.Ticket_closed)
)) dat
left join
(SELECT Status, Channel,
FORMAT_DATE("%b-%Y", Date) as Ticket_Open,
FORMAT_DATE("%b-%Y", Closing_Date) as Ticket_closed,
ifnull(count(distinct Requester_ID),0) as Complaints FROM {{var('zendesk_users')}}
group by 1,2,3,4) tick
on dat.Ticket_Open = tick.Ticket_Open
and dat.Ticket_closed = tick.Ticket_closed)

Union all
(
SELECT  FORMAT_DATE("%b-%Y", Date) as Ticket_Open, FORMAT_DATE("%b-%Y", Closing_Date) as Ticket_closed, status, channel,

ifnull(count(distinct Requester_ID),0) as Complaints FROM {{var('zendesk_users')}}
where Closing_Date is null
group by 1,2,3,4)