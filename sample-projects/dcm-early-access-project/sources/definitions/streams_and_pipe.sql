define table DCM_DEMO_1{{env_suffix}}.RAW.PIPE_ORDERS (
	ORDER_ID number,
	CUSTOMER_ID number,
	TRUCK_ID number,
	ORDER_TS timestamp_ntz,
	MENU_ITEM_ID number,
	QUANTITY number
)
;


define pipe DCM_DEMO_1{{env_suffix}}.RAW.ORDERS_PIPE
	auto_ingest = false
as
	copy into DCM_DEMO_1{{env_suffix}}.RAW.PIPE_ORDERS
	from @DCM_DEMO_1{{env_suffix}}.RAW.PUBLIC_S3_BUCKET/dcm_sample_orders/
	file_format = (format_name = 'DCM_DEMO_1{{env_suffix}}.RAW.DCM_DEMO_CSV')
;


define stream DCM_DEMO_1{{env_suffix}}.RAW.INCOMING_ORDERS_STREAM
	on table DCM_DEMO_1{{env_suffix}}.RAW.DAILY_ORDERS_INCOMING
;


define view DCM_DEMO_1{{env_suffix}}.RAW.V_CUSTOMER_CHANGES
	change_tracking = true
as
select
	CUSTOMER_ID,
	CITY
from
	DCM_DEMO_1{{env_suffix}}.RAW.CUSTOMER
;


define stream DCM_DEMO_1{{env_suffix}}.RAW.CUSTOMER_VIEW_STREAM
	on view DCM_DEMO_1{{env_suffix}}.RAW.V_CUSTOMER_CHANGES
;


define stream DCM_DEMO_1{{env_suffix}}.RAW.ORDER_FILES_STREAM
	on stage DCM_DEMO_1{{env_suffix}}.RAW.TASTY_BYTES_ORDERS_STAGE
;


{% if external_table %}
define stream DCM_DEMO_1{{env_suffix}}.RAW.EXTERNAL_TABLE_STREAM
	on external table {{external_table}}
	insert_only = true
;
{% endif %}