-- Infrastructure that exists before dbt runs for the first time.
-- dbt only writes into the STAGING and ANALYTICS schemas; DCM owns every container around them.

define database DCM_DBT_DEMO{{env_suffix}}
	comment = 'DCM + dbt demo: infrastructure managed by DCM, transformations by dbt';


define schema DCM_DBT_DEMO{{env_suffix}}.RAW
	comment = 'Landing zone, loaded by Snowpipe';


define schema DCM_DBT_DEMO{{env_suffix}}.STAGING
	comment = 'dbt staging and intermediate models, not exposed to consumers';


define schema DCM_DBT_DEMO{{env_suffix}}.ANALYTICS
	comment = 'dbt marts: the tables and Dynamic Tables that consumers read';


define schema DCM_DBT_DEMO{{env_suffix}}.SERVE
	comment = 'Consumption layer managed by DCM: views, semantic views, apps';


define schema DCM_DBT_DEMO{{env_suffix}}.GOVERNANCE
	comment = 'Tags, policies, and data quality functions';


define schema DCM_DBT_DEMO{{env_suffix}}.ORCHESTRATION
	comment = 'dbt project object and the task graph that runs the pipeline';


define warehouse DCM_DBT_DEMO{{env_suffix}}_WH
	with
		warehouse_size = '{{wh_size}}'
		auto_suspend = 60
		auto_resume = true
		initially_suspended = true
		comment = 'Runs ingestion, dbt, Dynamic Table refreshes, and the app';
