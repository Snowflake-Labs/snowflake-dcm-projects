/*=============================================================================
  infrastructure.sql — Database, schema, warehouse, roles, grants

  Only DCM Projects can create these as "managed entities" with full
  lifecycle (rename / drop on removal). Templating values come from
  manifest.yml so DEV and PROD deploy with different sizes and owners.
=============================================================================*/

DEFINE WAREHOUSE DCM_DEMO_4_WH{{env_suffix}}
WITH
    WAREHOUSE_SIZE = '{{wh_size}}'
    AUTO_SUSPEND = 60
    COMMENT = 'For Quickstart Demo of DCM Projects with Tasks';

DEFINE DATABASE DCM_DEMO_4{{env_suffix}}
    COMMENT = 'Quickstart Demo for DCM Projects with Tasks and task graphs';

DEFINE SCHEMA DCM_DEMO_4{{env_suffix}}.PIPELINE
    COMMENT = 'Task graph, helper functions, procedures, and DMFs';

DEFINE DATABASE ROLE DCM_DEMO_4{{env_suffix}}.ADMIN{{env_suffix}};
GRANT DATABASE ROLE DCM_DEMO_4{{env_suffix}}.ADMIN{{env_suffix}} TO ROLE {{project_owner_role}};

DEFINE ROLE DCM_DEMO_4{{env_suffix}}_READ;
GRANT USAGE ON DATABASE  DCM_DEMO_4{{env_suffix}}          TO ROLE DCM_DEMO_4{{env_suffix}}_READ;
GRANT USAGE ON SCHEMA    DCM_DEMO_4{{env_suffix}}.PIPELINE TO ROLE DCM_DEMO_4{{env_suffix}}_READ;
GRANT USAGE ON WAREHOUSE DCM_DEMO_4_WH{{env_suffix}}       TO ROLE DCM_DEMO_4{{env_suffix}}_READ;
GRANT SELECT ON ALL TABLES IN DATABASE DCM_DEMO_4{{env_suffix}} TO ROLE DCM_DEMO_4{{env_suffix}}_READ;
GRANT SELECT ON ALL VIEWS  IN DATABASE DCM_DEMO_4{{env_suffix}} TO ROLE DCM_DEMO_4{{env_suffix}}_READ;
