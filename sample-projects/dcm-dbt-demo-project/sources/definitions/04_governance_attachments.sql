{% if not initial_deployment %}
-- Attachments on tables that dbt creates. They need the dbt tables to exist,
-- so they deploy in the second phase of the first deployment.

attach tag DCM_DBT_DEMO{{env_suffix}}.GOVERNANCE.DATA_DOMAIN = 'customer'
	to table DCM_DBT_DEMO{{env_suffix}}.ANALYTICS.DIM_CUSTOMERS,
		dynamic table DCM_DBT_DEMO{{env_suffix}}.ANALYTICS.CUSTOMER_ORDER_SUMMARY;


attach tag DCM_DBT_DEMO{{env_suffix}}.GOVERNANCE.DATA_DOMAIN = 'sales'
	to dynamic table DCM_DBT_DEMO{{env_suffix}}.ANALYTICS.FCT_ORDERS;


attach tag DCM_DBT_DEMO{{env_suffix}}.GOVERNANCE.DATA_DOMAIN = 'product'
	to dynamic table DCM_DBT_DEMO{{env_suffix}}.ANALYTICS.DIM_PRODUCTS;


attach tag DCM_DBT_DEMO{{env_suffix}}.GOVERNANCE.PII = 'email'
	to table DCM_DBT_DEMO{{env_suffix}}.ANALYTICS.DIM_CUSTOMERS column EMAIL;


attach masking policy DCM_DBT_DEMO{{env_suffix}}.GOVERNANCE.EMAIL_MASK
	to table DCM_DBT_DEMO{{env_suffix}}.ANALYTICS.DIM_CUSTOMERS column EMAIL;


-- A Dynamic Table materializes with its owner's privileges, so the policy on
-- FCT_ORDERS does not filter the Dynamic Tables built on top of it.
-- Attach it to every table with a REGION column that analysts can read.
attach row access policy DCM_DBT_DEMO{{env_suffix}}.GOVERNANCE.REGION_FILTER
	to dynamic table DCM_DBT_DEMO{{env_suffix}}.ANALYTICS.FCT_ORDERS on (REGION),
		dynamic table DCM_DBT_DEMO{{env_suffix}}.ANALYTICS.CUSTOMER_ORDER_SUMMARY on (REGION),
		dynamic table DCM_DBT_DEMO{{env_suffix}}.SERVE.DAILY_REVENUE on (REGION);
{% endif %}
