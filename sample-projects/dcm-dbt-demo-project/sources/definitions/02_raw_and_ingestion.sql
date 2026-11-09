-- Raw layer: files land in an internal stage and Snowpipe copies them into raw tables.
-- The raw tables are dbt sources, so DCM creates them before dbt runs.

-- Reads the header row, so the pipes map file columns to table columns by name.
-- A column that DCM adds to a raw table is filled from new files without any
-- change to the pipes (a pipe's COPY statement can't be altered after creation).
define file format DCM_DBT_DEMO{{env_suffix}}.RAW.CSV_FORMAT
	type = csv
	parse_header = true
	error_on_column_count_mismatch = false
	field_optionally_enclosed_by = '"';


define stage DCM_DBT_DEMO{{env_suffix}}.RAW.LANDING
	directory = (enable = true)
	comment = 'Upload CSV files here, one folder per raw table: customers/, orders/, order_items/, products/';


define table DCM_DBT_DEMO{{env_suffix}}.RAW.CUSTOMERS (
	CUSTOMER_ID number,
	CUSTOMER_NAME varchar,
	EMAIL varchar,
	REGION varchar,
	UPDATED_AT timestamp_ntz
)
change_tracking = true
data_metric_schedule = 'TRIGGER_ON_CHANGES';


define table DCM_DBT_DEMO{{env_suffix}}.RAW.ORDERS (
	ORDER_ID number,
	CUSTOMER_ID number,
	ORDER_DATE date,
	STATUS varchar
)
change_tracking = true
data_metric_schedule = 'TRIGGER_ON_CHANGES';


define table DCM_DBT_DEMO{{env_suffix}}.RAW.ORDER_ITEMS (
	ORDER_ID number,
	LINE_NUMBER number,
	PRODUCT_ID number,
	QUANTITY number,
	UNIT_PRICE_CENTS number
)
change_tracking = true
data_metric_schedule = 'TRIGGER_ON_CHANGES';


define table DCM_DBT_DEMO{{env_suffix}}.RAW.PRODUCTS (
	PRODUCT_ID number,
	PRODUCT_NAME varchar,
	CATEGORY varchar,
	LIST_PRICE_CENTS number
)
change_tracking = true
data_metric_schedule = 'TRIGGER_ON_CHANGES';


{% for raw_table in ['CUSTOMERS', 'ORDERS', 'ORDER_ITEMS', 'PRODUCTS'] %}
define pipe DCM_DBT_DEMO{{env_suffix}}.RAW.LOAD_{{raw_table}}
	as
	copy into DCM_DBT_DEMO{{env_suffix}}.RAW.{{raw_table}}
	from @DCM_DBT_DEMO{{env_suffix}}.RAW.LANDING/{{raw_table | lower}}/
	file_format = (format_name = 'DCM_DBT_DEMO{{env_suffix}}.RAW.CSV_FORMAT')
	match_by_column_name = case_insensitive;

{% endfor %}

-- Data quality expectations on the dbt sources
attach data metric function SNOWFLAKE.CORE.NULL_COUNT
	to table DCM_DBT_DEMO{{env_suffix}}.RAW.ORDERS
	on (CUSTOMER_ID)
	expectation NO_ORPHAN_ORDERS (value = 0);


attach data metric function SNOWFLAKE.CORE.MIN
	to table DCM_DBT_DEMO{{env_suffix}}.RAW.ORDER_ITEMS
	on (UNIT_PRICE_CENTS)
	expectation NO_NEGATIVE_PRICES (value >= 0);


attach data metric function SNOWFLAKE.CORE.DUPLICATE_COUNT
	to table DCM_DBT_DEMO{{env_suffix}}.RAW.CUSTOMERS
	on (CUSTOMER_ID)
	expectation UNIQUE_CUSTOMERS (value = 0);


attach data metric function SNOWFLAKE.CORE.DUPLICATE_COUNT
	to table DCM_DBT_DEMO{{env_suffix}}.RAW.PRODUCTS
	on (PRODUCT_ID)
	expectation UNIQUE_PRODUCTS (value = 0);
