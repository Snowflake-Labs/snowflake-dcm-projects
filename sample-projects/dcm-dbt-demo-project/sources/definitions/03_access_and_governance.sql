-- Roles, tags, and policies. The policy objects exist before dbt runs;
-- attaching them to dbt tables happens in 04_governance_attachments.sql.

define role DCM_DBT_DEMO{{env_suffix}}_ANALYST
	comment = 'Reads the consumption layer; sees masked emails and only EMEA rows';


define role DCM_DBT_DEMO{{env_suffix}}_PII_READER
	comment = 'Sees unmasked emails and all regions';


grant role DCM_DBT_DEMO{{env_suffix}}_ANALYST to role {{project_owner_role}};
grant role DCM_DBT_DEMO{{env_suffix}}_PII_READER to role {{project_owner_role}};


grant usage on warehouse DCM_DBT_DEMO{{env_suffix}}_WH to role DCM_DBT_DEMO{{env_suffix}}_ANALYST;
grant usage on database DCM_DBT_DEMO{{env_suffix}} to role DCM_DBT_DEMO{{env_suffix}}_ANALYST;
grant usage on schema DCM_DBT_DEMO{{env_suffix}}.ANALYTICS to role DCM_DBT_DEMO{{env_suffix}}_ANALYST;
grant usage on schema DCM_DBT_DEMO{{env_suffix}}.SERVE to role DCM_DBT_DEMO{{env_suffix}}_ANALYST;


-- Inherited grants cover every current and future table in the schema,
-- including tables that dbt creates or replaces later.
grant inherited select on all tables in schema DCM_DBT_DEMO{{env_suffix}}.ANALYTICS
	to role DCM_DBT_DEMO{{env_suffix}}_ANALYST;
grant inherited select on all dynamic tables in schema DCM_DBT_DEMO{{env_suffix}}.ANALYTICS
	to role DCM_DBT_DEMO{{env_suffix}}_ANALYST;
grant inherited select on all views in schema DCM_DBT_DEMO{{env_suffix}}.SERVE
	to role DCM_DBT_DEMO{{env_suffix}}_ANALYST;
grant inherited select on all dynamic tables in schema DCM_DBT_DEMO{{env_suffix}}.SERVE
	to role DCM_DBT_DEMO{{env_suffix}}_ANALYST;


define tag DCM_DBT_DEMO{{env_suffix}}.GOVERNANCE.DATA_DOMAIN
	allowed_values 'sales', 'customer', 'product'
	comment = 'Business domain of the data';


define tag DCM_DBT_DEMO{{env_suffix}}.GOVERNANCE.PII
	allowed_values 'email', 'name'
	comment = 'Marks columns that contain personal data';


define tag DCM_DBT_DEMO{{env_suffix}}.GOVERNANCE.COST_CENTER
	comment = 'Cost attribution for compute';


attach tag DCM_DBT_DEMO{{env_suffix}}.GOVERNANCE.COST_CENTER = 'analytics-engineering'
	to warehouse DCM_DBT_DEMO{{env_suffix}}_WH;


define masking policy DCM_DBT_DEMO{{env_suffix}}.GOVERNANCE.EMAIL_MASK
	as (VAL varchar)
	returns varchar ->
	case
		when is_role_in_session('DCM_DBT_DEMO{{env_suffix}}_PII_READER') then VAL
		else regexp_replace(VAL, '.+@', '*****@')
	end
	comment = 'Full email for PII readers, domain only for everybody else';


define row access policy DCM_DBT_DEMO{{env_suffix}}.GOVERNANCE.REGION_FILTER
	as (REGION_VAL varchar)
	returns boolean ->
	is_role_in_session('DCM_DBT_DEMO{{env_suffix}}_PII_READER')
	or is_role_in_session('{{project_owner_role}}')
	or (is_role_in_session('DCM_DBT_DEMO{{env_suffix}}_ANALYST') and REGION_VAL = 'EMEA')
	comment = 'Analysts see EMEA rows only';
