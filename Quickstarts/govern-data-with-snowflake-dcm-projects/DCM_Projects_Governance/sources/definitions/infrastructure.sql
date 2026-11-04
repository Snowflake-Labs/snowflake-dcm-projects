/*=============================================================================
  infrastructure.sql: Warehouse, database, schemas, roles, and grants

  DCM manages these as first-class entities. The project object itself lives in
  DCM_DEMO.PROJECTS (created by the first plan); this project defines a
  separate database, DCM_DEMO_5, for the governance demo.
=============================================================================*/

-- Runs the queries in the post-deploy and verify scripts, for both the
-- developer and the analyst role. It is the only warehouse the analyst role
-- can use, so the scripts select it with USE WAREHOUSE after every USE ROLE.
DEFINE WAREHOUSE DCM_DEMO_5_WH{{env_suffix}}
WITH
    WAREHOUSE_SIZE = '{{wh_size}}'
    AUTO_SUSPEND = 300
    COMMENT = 'For the Security & Governance Quickstart';

DEFINE DATABASE DCM_DEMO_5{{env_suffix}}
    COMMENT = 'Security & Governance Quickstart';

DEFINE SCHEMA DCM_DEMO_5{{env_suffix}}.RAW    COMMENT = 'Source data, including PII';
DEFINE SCHEMA DCM_DEMO_5{{env_suffix}}.GOV    COMMENT = 'Governance objects: tags and policies';

-- A restricted analyst role: can read the data but is NOT privileged to see PII.
DEFINE ROLE DCM_DEMO_5{{env_suffix}}_ANALYST
    COMMENT = 'Restricted analyst role for the Security & Governance Quickstart';
GRANT USAGE ON DATABASE  DCM_DEMO_5{{env_suffix}}       TO ROLE DCM_DEMO_5{{env_suffix}}_ANALYST;
GRANT USAGE ON SCHEMA    DCM_DEMO_5{{env_suffix}}.RAW   TO ROLE DCM_DEMO_5{{env_suffix}}_ANALYST;
GRANT USAGE ON WAREHOUSE DCM_DEMO_5_WH{{env_suffix}}   TO ROLE DCM_DEMO_5{{env_suffix}}_ANALYST;

-- Inherited grants are a one-time account opt-in, turned on in
-- scripts/01_pre_deploy.sql:
-- ALTER ACCOUNT SET FEATURE_RBAC_INHERITED_GRANTS = 'ENABLED';
-- https://docs.snowflake.com/en/release-notes/2026/other/2026-09-10-inherited-grants-ga
GRANT INHERITED SELECT ON ALL TABLES IN DATABASE DCM_DEMO_5{{env_suffix}} TO ROLE DCM_DEMO_5{{env_suffix}}_ANALYST;
GRANT INHERITED SELECT ON ALL VIEWS  IN DATABASE DCM_DEMO_5{{env_suffix}} TO ROLE DCM_DEMO_5{{env_suffix}}_ANALYST;
