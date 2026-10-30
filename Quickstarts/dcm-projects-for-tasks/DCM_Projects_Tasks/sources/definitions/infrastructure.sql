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

DEFINE DATABASE DCM_DEMO_4{{env_suffix}}
    COMMENT = 'DCM Projects for Tasks quickstart: task graph and its tables';

DEFINE SCHEMA DCM_DEMO_4{{env_suffix}}.PIPELINE
    COMMENT = 'Tables, member tasks and the task graph';
