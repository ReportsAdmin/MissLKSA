select OrderId  ,cast(RefundStatus as string) RefundStatus,	cast(ReturnReason as string) ReturnReason,
     cast(ReturnRequestID as string) ReturnRequestID,cast(ReturnStatus as string) ReturnStatus,	cast(userId as string) userId,
     'MissL' Halo_Country

from(

select 
ord.increment_id OrderId,
--ret_id.reason_id,


null ReturnReason,

null ReturnRequestID,
null userId,
null OrderItemId,
null Requirement,
null ReturnStatus,
null RequestType,
null RefundStatus,
null ReturnQuantity,
null ReceivedQuantity,
null RefundAmount,
null RefundedAmount,
null UpdatedDate
from `noted-computing-279322.halo_1_1.magento_transaction` as ord
---Record count incresred as each order_id can have multiple track_number or title

)