{{
	config(
		materialized = 'incremental',
		incremental_strategy = 'merge',
		unique_key = 'CUSTOMER_ID',
		on_schema_change = 'append_new_columns'
	)
}}

select
	CUSTOMER_ID,
	trim(CUSTOMER_NAME) as CUSTOMER_NAME,
	lower(EMAIL) as EMAIL,
	upper(REGION) as REGION,
	UPDATED_AT
from {{ source('raw', 'CUSTOMERS') }}
{{ incremental_watermark('UPDATED_AT') }}
qualify row_number() over (partition by CUSTOMER_ID order by UPDATED_AT desc) = 1
