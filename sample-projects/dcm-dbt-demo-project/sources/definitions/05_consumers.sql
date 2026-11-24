{% if not initial_deployment %}
-- Consumption layer on top of the dbt output, managed by DCM.
-- DOWNSTREAM lag: the Dynamic Tables refresh only when the task graph refreshes them.

define dynamic table DCM_DBT_DEMO{{env_suffix}}.SERVE.DAILY_REVENUE
	warehouse = DCM_DBT_DEMO{{env_suffix}}_WH
	target_lag = 'DOWNSTREAM'
	refresh_mode = incremental
	initialize = on_schedule
	data_metric_schedule = 'TRIGGER_ON_CHANGES'
as
select
	ORDER_DATE,
	REGION,
	count(*) as ORDER_COUNT,
	sum(AMOUNT) as REVENUE
from DCM_DBT_DEMO{{env_suffix}}.ANALYTICS.FCT_ORDERS
group by ORDER_DATE, REGION;


attach data metric function SNOWFLAKE.CORE.NULL_COUNT
	to dynamic table DCM_DBT_DEMO{{env_suffix}}.SERVE.DAILY_REVENUE
	on (REGION)
	expectation EVERY_ORDER_HAS_A_REGION (value = 0);


define view DCM_DBT_DEMO{{env_suffix}}.SERVE.CUSTOMER_DIRECTORY
as
select
	CUSTOMER_ID,
	CUSTOMER_NAME,
	EMAIL,
	REGION
from DCM_DBT_DEMO{{env_suffix}}.ANALYTICS.DIM_CUSTOMERS;


define semantic view DCM_DBT_DEMO{{env_suffix}}.SERVE.SALES
	tables (
		ORDERS as DCM_DBT_DEMO{{env_suffix}}.ANALYTICS.FCT_ORDERS
			primary key (ORDER_ID),
		CUSTOMERS as DCM_DBT_DEMO{{env_suffix}}.ANALYTICS.DIM_CUSTOMERS
			primary key (CUSTOMER_ID)
	)
	relationships (
		ORDERS_TO_CUSTOMERS as ORDERS (CUSTOMER_ID) references CUSTOMERS (CUSTOMER_ID)
	)
	dimensions (
		ORDERS.ORDER_DATE as ORDERS.ORDER_DATE,
		CUSTOMERS.REGION as CUSTOMERS.REGION,
		CUSTOMERS.CUSTOMER_NAME as CUSTOMERS.CUSTOMER_NAME
	)
	metrics (
		ORDERS.TOTAL_REVENUE as sum(ORDERS.AMOUNT),
		ORDERS.ORDER_COUNT as count(ORDERS.ORDER_ID)
	)
	comment = 'Revenue and order metrics for Cortex Analyst';


grant select on semantic view DCM_DBT_DEMO{{env_suffix}}.SERVE.SALES
	to role DCM_DBT_DEMO{{env_suffix}}_ANALYST;


define streamlit DCM_DBT_DEMO{{env_suffix}}.SERVE.REVENUE_APP
	from 'asset://revenue_app/'
	main_file = 'streamlit_app.py'
	query_warehouse = DCM_DBT_DEMO{{env_suffix}}_WH
	title = 'Daily revenue';


grant usage on streamlit DCM_DBT_DEMO{{env_suffix}}.SERVE.REVENUE_APP
	to role DCM_DBT_DEMO{{env_suffix}}_ANALYST;
{% endif %}
