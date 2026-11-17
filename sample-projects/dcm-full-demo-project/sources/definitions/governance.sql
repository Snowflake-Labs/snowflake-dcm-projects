define schema DCM_DEMO_1{{env_suffix}}.GOV;

-- Network policy demo: this only CREATES the objects; it does NOT activate them.
-- Existing account/user-level network policies are unaffected.
define network rule DCM_DEMO_1{{env_suffix}}.GOV.ALLOW_ALL_IPS
    MODE = INGRESS
    TYPE = IPV4
    VALUE_LIST = ('0.0.0.0/0')
;

define network policy DCM_DEMO_1{{env_suffix}}_POLICY
    allowed_network_rule_list = ('DCM_DEMO_1{{env_suffix}}.GOV.ALLOW_ALL_IPS')
    comment = 'Demo-only policy - allows all IPs, never auto-activated'
;

define authentication policy DCM_DEMO_1{{env_suffix}}.GOV.GITHUB_AUTH_POLICY
    authentication_methods = ('PROGRAMMATIC_ACCESS_TOKEN')
    pat_policy = ( 
        default_expiry_in_days=15,
        max_expiry_in_days=90,
        network_policy_evaluation = ENFORCED_NOT_REQUIRED
    )
;


define masking policy DCM_DEMO_1{{env_suffix}}.GOV.EMAIL_MASK
    as (VAL string) returns string ->
    case
        when is_role_in_session('DCM_DEMO_1_ADMIN{{env_suffix}}') then VAL
        when current_role() in ('DCM_DEVELOPER') then regexp_replace(VAL, '.+\\@', '*****@')
        else '***MASKED***'
    end
    comment = 'Masks email addresses for non-privileged roles'
;


-- Masks customer last names
define masking policy DCM_DEMO_1{{env_suffix}}.GOV.PII_NAME_MASK
    as (VAL string) returns string ->
    case
        when is_role_in_session('DCM_DEMO_1_ADMIN{{env_suffix}}') then VAL
        when current_role() in ('DCM_DEVELOPER') then left(VAL, 1) || '*****'
        else '***MASKED***'
    end
    comment = 'Masks customer last names for non-privileged roles'
;

attach masking policy DCM_DEMO_1{{env_suffix}}.GOV.PII_NAME_MASK
	to table DCM_DEMO_1{{env_suffix}}.RAW.CUSTOMER
		column LAST_NAME
;

---------------------------------

define tag DCM_DEMO_1{{env_suffix}}.GOV.PII
    allowed_values 'PII'
;

define tag DCM_DEMO_1{{env_suffix}}.GOV.DATA_DOMAIN
    allowed_values 'SALES', 'MARKETING', 'FINANCE', 'HR', 'CUSTOMER'
;

-- Tag propagation (Enterprise Edition): downstream objects that read this column inherit the tag
define tag DCM_DEMO_1{{env_suffix}}.GOV.SENSITIVITY
    allowed_values 'CONFIDENTIAL', 'INTERNAL'
    propagate = on_dependency_and_data_movement
    on_conflict = allowed_values_sequence
;

attach tag DCM_DEMO_1{{env_suffix}}.GOV.SENSITIVITY = 'CONFIDENTIAL'
    to table DCM_DEMO_1{{env_suffix}}.RAW.CUSTOMER
        column LAST_NAME
;
   
attach tag DCM_DEMO_1{{env_suffix}}.GOV.PII = 'PII'
    to dynamic table DCM_DEMO_1{{env_suffix}}.ANALYTICS.ENRICHED_ORDER_DETAILS
        column CUSTOMER_CITY
;

attach tag DCM_DEMO_1{{env_suffix}}.GOV.DATA_DOMAIN = 'CUSTOMER'
    to table DCM_DEMO_1{{env_suffix}}.RAW.CUSTOMER
;

attach tag DCM_DEMO_1{{env_suffix}}.GOV.DATA_DOMAIN = 'MARKETING'
    to dynamic table DCM_DEMO_1{{env_suffix}}.ANALYTICS.ENRICHED_ORDER_DETAILS
;

attach tag DCM_DEMO_1{{env_suffix}}.GOV.PII = 'PII'
    to table DCM_DEMO_1{{env_suffix}}.RAW.CUSTOMER
        column LAST_NAME
;


-- Restricts customer rows to the cities of a regional sales role
define row access policy DCM_DEMO_1{{env_suffix}}.GOV.CITY_ROW_FILTER
	as (CITY_VALUE varchar) returns boolean ->
	case
		when is_role_in_session('DCM_DEMO_1_ADMIN{{env_suffix}}') then true
		when current_role() = 'DCM_DEMO_1_SALES_NA{{env_suffix}}'
			and CITY_VALUE in ('New York', 'Chicago', 'San Francisco', 'Austin') then true
		when current_role() = 'DCM_DEMO_1_SALES_EU{{env_suffix}}'
			and CITY_VALUE in ('London') then true
		else false
	end
	comment = 'Filters customer rows by city based on the active regional sales role'
;

attach row access policy DCM_DEMO_1{{env_suffix}}.GOV.CITY_ROW_FILTER
	to table DCM_DEMO_1{{env_suffix}}.RAW.CUSTOMER on (CITY)
;


attach tag DCM_DEMO_1{{env_suffix}}.GOV.DATA_DOMAIN = 'SALES'
	to database DCM_DEMO_1{{env_suffix}},
		schema DCM_DEMO_1{{env_suffix}}.SERVE,
		dynamic table DCM_DEMO_1{{env_suffix}}.ANALYTICS.TRUCK_PERFORMANCE,
		view DCM_DEMO_1{{env_suffix}}.SERVE.V_DASHBOARD_DAILY_SALES,
		function DCM_DEMO_1{{env_suffix}}.ANALYTICS.CALCULATE_PROFIT_MARGIN(number, number),
		function DCM_DEMO_1{{env_suffix}}.RAW.INVENTORY_SPREAD(table(number)),
		procedure DCM_DEMO_1{{env_suffix}}.ANALYTICS.SP_CLASSIFY_CUSTOMER_LOYALTY(),
		stage DCM_DEMO_1{{env_suffix}}.RAW.TASTY_BYTES_ORDERS_STAGE,
		task DCM_DEMO_1{{env_suffix}}.RAW.INSERT_SAMPLE_DATA,
		role DCM_DEMO_1{{env_suffix}}_READ,
		warehouse DCM_DEMO_1_WH{{env_suffix}}
;


attach tag DCM_DEMO_1{{env_suffix}}.GOV.PII = 'PII',
	DCM_DEMO_1{{env_suffix}}.GOV.DATA_DOMAIN = 'CUSTOMER'
	to table DCM_DEMO_1{{env_suffix}}.RAW.CUSTOMER column CITY,
		view DCM_DEMO_1{{env_suffix}}.SERVE.V_DASHBOARD_SALES_BY_CATEGORY_CITY column CUSTOMER_CITY
;
