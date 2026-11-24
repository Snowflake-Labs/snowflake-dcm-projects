select
	ORDER_ID,
	CUSTOMER_ID,
	ORDER_DATE,
	upper(STATUS) as STATUS
from {{ source('raw', 'ORDERS') }}
where ORDER_ID is not null
