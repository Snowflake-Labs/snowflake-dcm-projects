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
	CUSTOMER_NAME,
	EMAIL,
	REGION,
	UPDATED_AT
from {{ ref('STG_CUSTOMERS') }}
{{ incremental_watermark('UPDATED_AT') }}
