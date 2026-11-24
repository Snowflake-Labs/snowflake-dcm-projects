-- Completed orders with their revenue and customer attributes.
with ORDER_TOTALS as (
	select
		ORDER_ID,
		count(*) as LINE_COUNT,
		sum(LINE_AMOUNT) as AMOUNT
	from {{ ref('INT_ORDER_ITEMS_ENRICHED') }}
	group by ORDER_ID
)

select
	O.ORDER_ID,
	O.ORDER_DATE,
	T.LINE_COUNT,
	T.AMOUNT,
	O.CUSTOMER_ID,
	C.CUSTOMER_NAME,
	C.REGION
from {{ ref('STG_ORDERS') }} as O
join ORDER_TOTALS as T
	on O.ORDER_ID = T.ORDER_ID
join {{ ref('DIM_CUSTOMERS') }} as C
	on O.CUSTOMER_ID = C.CUSTOMER_ID
where O.STATUS = 'COMPLETED'
