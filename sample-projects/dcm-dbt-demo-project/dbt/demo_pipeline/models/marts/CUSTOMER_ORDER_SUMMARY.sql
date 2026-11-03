-- Order history per customer, e.g. for segmentation.
select
	CUSTOMER_ID,
	CUSTOMER_NAME,
	REGION,
	count(*) as ORDER_COUNT,
	sum(AMOUNT) as LIFETIME_REVENUE,
	min(ORDER_DATE) as FIRST_ORDER_DATE,
	max(ORDER_DATE) as LAST_ORDER_DATE
from {{ ref('FCT_ORDERS') }}
group by CUSTOMER_ID, CUSTOMER_NAME, REGION
