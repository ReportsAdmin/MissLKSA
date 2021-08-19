
select row_number() over() channel_id,channel,{{Country}} Halo_Country
from
(
select distinct source_medium channel
from `{{refCampaigns}}`
)
