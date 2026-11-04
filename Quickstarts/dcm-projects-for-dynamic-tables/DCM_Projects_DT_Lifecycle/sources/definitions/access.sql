DEFINE WAREHOUSE DCM_DEMO_3_WH{{env_suffix}}
WITH
    WAREHOUSE_SIZE = '{{wh_size}}'
    AUTO_SUSPEND = 60
    COMMENT = 'For Quickstart Demo of DCM Projects with Dynamic Tables';

DEFINE DATABASE ROLE DCM_DEMO_3{{env_suffix}}.ADMIN{{env_suffix}}
    COMMENT = 'Database-scoped admin role, granted to the project owner';
GRANT DATABASE ROLE DCM_DEMO_3{{env_suffix}}.ADMIN{{env_suffix}} TO ROLE {{project_owner_role}};

DEFINE ROLE DCM_DEMO_3{{env_suffix}}_READ
    COMMENT = 'Read access to every table, dynamic table and view in the demo database';

GRANT USAGE ON DATABASE DCM_DEMO_3{{env_suffix}}         TO ROLE DCM_DEMO_3{{env_suffix}}_READ;
GRANT USAGE ON SCHEMA DCM_DEMO_3{{env_suffix}}.RAW       TO ROLE DCM_DEMO_3{{env_suffix}}_READ;
GRANT USAGE ON SCHEMA DCM_DEMO_3{{env_suffix}}.ANALYTICS TO ROLE DCM_DEMO_3{{env_suffix}}_READ;
GRANT USAGE ON SCHEMA DCM_DEMO_3{{env_suffix}}.SERVE     TO ROLE DCM_DEMO_3{{env_suffix}}_READ;
GRANT USAGE ON WAREHOUSE DCM_DEMO_3_WH{{env_suffix}}     TO ROLE DCM_DEMO_3{{env_suffix}}_READ;

-- Inherited grants need a one-time account opt-in, made in scripts/01_pre_deploy.sql:
-- ALTER ACCOUNT SET FEATURE_RBAC_INHERITED_GRANTS = 'ENABLED';
GRANT INHERITED SELECT ON ALL TABLES IN DATABASE DCM_DEMO_3{{env_suffix}}         TO ROLE DCM_DEMO_3{{env_suffix}}_READ;
GRANT INHERITED SELECT ON ALL DYNAMIC TABLES IN DATABASE DCM_DEMO_3{{env_suffix}} TO ROLE DCM_DEMO_3{{env_suffix}}_READ;
GRANT INHERITED SELECT ON ALL VIEWS IN DATABASE DCM_DEMO_3{{env_suffix}}          TO ROLE DCM_DEMO_3{{env_suffix}}_READ;
