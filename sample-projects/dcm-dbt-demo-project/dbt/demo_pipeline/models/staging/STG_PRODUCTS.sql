select
	PRODUCT_ID,
	trim(PRODUCT_NAME) as PRODUCT_NAME,
	upper(CATEGORY) as CATEGORY,
	{{ cents_to_currency('LIST_PRICE_CENTS') }} as LIST_PRICE
from {{ source('raw', 'PRODUCTS') }}
