-- One row per order line, with the product attributes and the line amount.
select
	I.ORDER_ID,
	I.LINE_NUMBER,
	I.PRODUCT_ID,
	P.PRODUCT_NAME,
	P.CATEGORY,
	I.QUANTITY,
	I.UNIT_PRICE,
	I.QUANTITY * I.UNIT_PRICE as LINE_AMOUNT
from {{ ref('STG_ORDER_ITEMS') }} as I
join {{ ref('STG_PRODUCTS') }} as P
	on I.PRODUCT_ID = P.PRODUCT_ID
