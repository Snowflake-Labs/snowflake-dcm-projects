define schema DCM_DEMO_1{{env_suffix}}.DBT_TRANSFORM
;


define dbt project DCM_DEMO_1{{env_suffix}}.DBT_TRANSFORM.TASTY_DBT
	from 'sources/dbt/tasty_dbt'
	default_target = '{{dbt_env}}'
;


define task DCM_DEMO_1{{env_suffix}}.DBT_TRANSFORM.DBT_RUN
	warehouse = DCM_DEMO_1_WH{{env_suffix}}
	schedule = '60 MINUTE'
	suspended
as
	execute dbt project DCM_DEMO_1{{env_suffix}}.DBT_TRANSFORM.TASTY_DBT
		args = 'run --target {{dbt_env}}'
;


define task DCM_DEMO_1{{env_suffix}}.DBT_TRANSFORM.DBT_TEST
	warehouse = DCM_DEMO_1_WH{{env_suffix}}
	after DCM_DEMO_1{{env_suffix}}.DBT_TRANSFORM.DBT_RUN
	suspended
as
	execute dbt project DCM_DEMO_1{{env_suffix}}.DBT_TRANSFORM.TASTY_DBT
		args = 'test --target {{dbt_env}}'
;


grant usage on dbt project DCM_DEMO_1{{env_suffix}}.DBT_TRANSFORM.TASTY_DBT to role DCM_DEMO_1_ADMIN{{env_suffix}};