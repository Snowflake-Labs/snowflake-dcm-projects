{% if not initial_deployment %}
-- Orchestration: load new files, check source freshness and merge the customer
-- models in parallel, apply the Dynamic Table definitions, refresh them, then
-- test the fresh data. A failing step stops only the tasks that depend on it.
--
-- dbt moves data in two ways here:
-- * Incremental models (STG_CUSTOMERS, DIM_CUSTOMERS) merge new rows during the dbt run.
-- * Dynamic Table models only get their definitions from dbt. All of them use
--   target_lag = 'DOWNSTREAM', so they refresh only when REFRESH_DYNAMIC_TABLES
--   refreshes the last tables in the chain, and Snowflake refreshes every
--   upstream Dynamic Table in the same operation.
--
-- reprocess_days is a run-time parameter: it comes from the task graph CONFIG
-- and is passed to dbt with --vars. Override it for one run with
-- EXECUTE TASK GRAPH ... USING CONFIG = $${"reprocess_days": "30"}$$;

define task DCM_DBT_DEMO{{env_suffix}}.ORCHESTRATION.LOAD_RAW
	warehouse = DCM_DBT_DEMO{{env_suffix}}_WH
as
begin
	alter pipe DCM_DBT_DEMO{{env_suffix}}.RAW.LOAD_CUSTOMERS refresh;
	alter pipe DCM_DBT_DEMO{{env_suffix}}.RAW.LOAD_ORDERS refresh;
	alter pipe DCM_DBT_DEMO{{env_suffix}}.RAW.LOAD_ORDER_ITEMS refresh;
	alter pipe DCM_DBT_DEMO{{env_suffix}}.RAW.LOAD_PRODUCTS refresh;
end;


define task DCM_DBT_DEMO{{env_suffix}}.ORCHESTRATION.CHECK_FRESHNESS
	warehouse = DCM_DBT_DEMO{{env_suffix}}_WH
	after DCM_DBT_DEMO{{env_suffix}}.ORCHESTRATION.LOAD_RAW
as
	execute dbt project DCM_DBT_DEMO{{env_suffix}}.ORCHESTRATION.DEMO_PIPELINE
		args = 'source freshness --target {{dbt_target}}'
		writeback = false;


define task DCM_DBT_DEMO{{env_suffix}}.ORCHESTRATION.BUILD_CUSTOMERS
	warehouse = DCM_DBT_DEMO{{env_suffix}}_WH
	after DCM_DBT_DEMO{{env_suffix}}.ORCHESTRATION.LOAD_RAW
as
declare
	DBT_ARGS varchar;
begin
	DBT_ARGS := 'build --selector incremental_models --target {{dbt_target}} --vars ''{"reprocess_days": '
		|| coalesce(SYSTEM$GET_TASK_GRAPH_CONFIG('reprocess_days')::varchar, '0') || '}''';
	execute dbt project DCM_DBT_DEMO{{env_suffix}}.ORCHESTRATION.DEMO_PIPELINE
		args = :DBT_ARGS
		writeback = false;
end;


define task DCM_DBT_DEMO{{env_suffix}}.ORCHESTRATION.APPLY_DYNAMIC_TABLES
	warehouse = DCM_DBT_DEMO{{env_suffix}}_WH
	after DCM_DBT_DEMO{{env_suffix}}.ORCHESTRATION.CHECK_FRESHNESS,
		DCM_DBT_DEMO{{env_suffix}}.ORCHESTRATION.BUILD_CUSTOMERS
as
	execute dbt project DCM_DBT_DEMO{{env_suffix}}.ORCHESTRATION.DEMO_PIPELINE
		args = 'run --selector dynamic_tables --target {{dbt_target}}';


define task DCM_DBT_DEMO{{env_suffix}}.ORCHESTRATION.REFRESH_DYNAMIC_TABLES
	warehouse = DCM_DBT_DEMO{{env_suffix}}_WH
	after DCM_DBT_DEMO{{env_suffix}}.ORCHESTRATION.APPLY_DYNAMIC_TABLES
as
	alter dynamic table DCM_DBT_DEMO{{env_suffix}}.SERVE.DAILY_REVENUE,
		DCM_DBT_DEMO{{env_suffix}}.ANALYTICS.CUSTOMER_ORDER_SUMMARY,
		DCM_DBT_DEMO{{env_suffix}}.ANALYTICS.DIM_PRODUCTS
		refresh;


define task DCM_DBT_DEMO{{env_suffix}}.ORCHESTRATION.TEST_DYNAMIC_TABLES
	warehouse = DCM_DBT_DEMO{{env_suffix}}_WH
	after DCM_DBT_DEMO{{env_suffix}}.ORCHESTRATION.REFRESH_DYNAMIC_TABLES
as
	execute dbt project DCM_DBT_DEMO{{env_suffix}}.ORCHESTRATION.DEMO_PIPELINE
		args = 'test --selector dynamic_tables --target {{dbt_target}}';


define task graph DCM_DBT_DEMO{{env_suffix}}.ORCHESTRATION.DAILY_PIPELINE
	schedule = 'USING CRON 0 6 * * * UTC'
	roots = (DCM_DBT_DEMO{{env_suffix}}.ORCHESTRATION.LOAD_RAW)
	config = '{"reprocess_days": "0"}'
	suspended
;
{% endif %}
