select
Lower (user_id) User_id,
customer_reference_id, {{var('Country')}} Halo_Country
from
{{var('fOrders')}}