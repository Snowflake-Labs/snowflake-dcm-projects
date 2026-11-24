select
	ORDER_ID,
	LINE_NUMBER,
	PRODUCT_ID,
	QUANTITY,
	{{ cents_to_currency('UNIT_PRICE_CENTS') }} as UNIT_PRICE
from {{ source('raw', 'ORDER_ITEMS') }}
where QUANTITY > 0
