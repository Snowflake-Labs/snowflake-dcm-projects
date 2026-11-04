/*=============================================================================
  infrastructure.sql: warehouse, database and schema for the task graph

  Templating values come from manifest.yml, so the DEV and PROD targets deploy
  the same objects with different names and sizes.
=============================================================================*/

DEFINE WAREHOUSE DCM_DEMO_4_WH{{env_suffix}}
WITH
    WAREHOUSE_SIZE = '{{wh_size}}'
    AUTO_SUSPEND = 60
    COMMENT = 'Runs the member tasks of the DCM Projects for Tasks quickstart';

-- LOG_EVENT_LEVEL = INFO makes every task in the database log its run status to
-- the event table, which is what the TASKS_ERROR_RATE alert in alerts.sql reads.
DEFINE DATABASE DCM_DEMO_4{{env_suffix}}
    LOG_EVENT_LEVEL = INFO
    COMMENT = 'DCM Projects for Tasks quickstart: task graph and its tables';

DEFINE SCHEMA DCM_DEMO_4{{env_suffix}}.PUBLIC
    COMMENT = 'Default schema, declared so DCM manages it';

DEFINE SCHEMA DCM_DEMO_4{{env_suffix}}.PIPELINE
    COMMENT = 'Tables, member tasks, the task graph and its alert';
