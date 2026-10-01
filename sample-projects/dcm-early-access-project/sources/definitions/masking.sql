define masking policy DCM_DEMO_1{{env_suffix}}.GOV.CITY_MASK
	as (CITY_VALUE varchar) returns varchar ->
		case
			when is_role_in_session('DCM_DEMO_1_ADMIN{{env_suffix}}') then CITY_VALUE
			else 'MASKED'
		end
;


attach masking policy DCM_DEMO_1{{env_suffix}}.GOV.CITY_MASK
	to table DCM_DEMO_1{{env_suffix}}.RAW.CUSTOMER column CITY,
		view DCM_DEMO_1{{env_suffix}}.SERVE.V_DASHBOARD_SALES_BY_CATEGORY_CITY column CUSTOMER_CITY,
		dynamic table DCM_DEMO_1{{env_suffix}}.ANALYTICS.ENRICHED_ORDER_DETAILS column CUSTOMER_CITY,
		dynamic table DCM_DEMO_1{{env_suffix}}.ANALYTICS.CUSTOMER_SPENDING_SUMMARY column CUSTOMER_CITY
;


define masking policy DCM_DEMO_1{{env_suffix}}.GOV.NAME_BY_CUSTOMER_ID
	as (NAME_VALUE varchar, CUSTOMER_ID_VALUE number) returns varchar ->
		case
			when is_role_in_session('DCM_DEMO_1_ADMIN{{env_suffix}}') and CUSTOMER_ID_VALUE > 0 then NAME_VALUE
			else 'MASKED'
		end
;


attach masking policy DCM_DEMO_1{{env_suffix}}.GOV.NAME_BY_CUSTOMER_ID
	to table DCM_DEMO_1{{env_suffix}}.RAW.CUSTOMER column FIRST_NAME using (FIRST_NAME, CUSTOMER_ID)
;