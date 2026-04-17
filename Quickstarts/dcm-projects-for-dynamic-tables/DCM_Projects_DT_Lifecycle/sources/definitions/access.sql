DEFINE WAREHOUSE DCM_DEMO_1_WH{{env_suffix}}
WITH
    WAREHOUSE_SIZE = '{{wh_size}}'
    AUTO_SUSPEND = 300
    COMMENT = 'For Quickstart Demo of DCM Projects with Dynamic Tables';

DEFINE DATABASE ROLE DCM_DEMO_1{{env_suffix}}.ADMIN{{env_suffix}};
GRANT DATABASE ROLE DCM_DEMO_1{{env_suffix}}.ADMIN{{env_suffix}} TO ROLE {{project_owner_role}};
DEFINE ROLE DCM_DEMO_1{{env_suffix}}_READ;

GRANT USAGE ON DATABASE DCM_DEMO_1{{env_suffix}}         TO ROLE DCM_DEMO_1{{env_suffix}}_READ;
GRANT USAGE ON SCHEMA DCM_DEMO_1{{env_suffix}}.RAW       TO ROLE DCM_DEMO_1{{env_suffix}}_READ;
GRANT USAGE ON SCHEMA DCM_DEMO_1{{env_suffix}}.ANALYTICS TO ROLE DCM_DEMO_1{{env_suffix}}_READ;
GRANT USAGE ON SCHEMA DCM_DEMO_1{{env_suffix}}.SERVE     TO ROLE DCM_DEMO_1{{env_suffix}}_READ;
GRANT USAGE ON WAREHOUSE DCM_DEMO_1_WH{{env_suffix}}     TO ROLE DCM_DEMO_1{{env_suffix}}_READ;

GRANT SELECT ON ALL TABLES IN DATABASE DCM_DEMO_1{{env_suffix}}         TO ROLE DCM_DEMO_1{{env_suffix}}_READ;
GRANT SELECT ON ALL DYNAMIC TABLES IN DATABASE DCM_DEMO_1{{env_suffix}} TO ROLE DCM_DEMO_1{{env_suffix}}_READ;
GRANT SELECT ON ALL VIEWS IN DATABASE DCM_DEMO_1{{env_suffix}}          TO ROLE DCM_DEMO_1{{env_suffix}}_READ;
