/*=============================================================================
  infrastructure.sql — Warehouse, database, schemas, roles, and grants

  DCM manages these as first-class entities. The project object itself lives in
  DCM_DEMO.PROJECTS (created in 01_pre_deploy.sql); this project defines a
  separate database, DCM_DEMO_5, for the governance demo.
=============================================================================*/

DEFINE WAREHOUSE DCM_DEMO_5_WH{{env_suffix}}
WITH
    WAREHOUSE_SIZE = '{{wh_size}}'
    AUTO_SUSPEND = 300
    COMMENT = 'For the Security & Governance Quickstart';

DEFINE DATABASE DCM_DEMO_5{{env_suffix}}
    COMMENT = 'Security & Governance Quickstart';

DEFINE SCHEMA DCM_DEMO_5{{env_suffix}}.RAW    COMMENT = 'Source data, including PII';
DEFINE SCHEMA DCM_DEMO_5{{env_suffix}}.GOV    COMMENT = 'Governance objects: tags and policies';
DEFINE SCHEMA DCM_DEMO_5{{env_suffix}}.SERVE  COMMENT = 'Consumer views';

-- A restricted analyst role: can read the data but is NOT privileged to see PII.
DEFINE ROLE DCM_DEMO_5{{env_suffix}}_ANALYST;
GRANT USAGE ON DATABASE  DCM_DEMO_5{{env_suffix}}       TO ROLE DCM_DEMO_5{{env_suffix}}_ANALYST;
GRANT USAGE ON SCHEMA    DCM_DEMO_5{{env_suffix}}.RAW   TO ROLE DCM_DEMO_5{{env_suffix}}_ANALYST;
GRANT USAGE ON SCHEMA    DCM_DEMO_5{{env_suffix}}.SERVE TO ROLE DCM_DEMO_5{{env_suffix}}_ANALYST;
GRANT USAGE ON WAREHOUSE DCM_DEMO_5_WH{{env_suffix}}   TO ROLE DCM_DEMO_5{{env_suffix}}_ANALYST;

-- Inherited grants require the following account-level opt-in (Public Preview),
-- enabled in scripts/01_pre_deploy.sql:
-- ALTER ACCOUNT SET FEATURE_RBAC_INHERITED_GRANTS = 'ENABLED';
GRANT INHERITED SELECT ON ALL TABLES IN DATABASE DCM_DEMO_5{{env_suffix}} TO ROLE DCM_DEMO_5{{env_suffix}}_ANALYST;
GRANT INHERITED SELECT ON ALL VIEWS  IN DATABASE DCM_DEMO_5{{env_suffix}} TO ROLE DCM_DEMO_5{{env_suffix}}_ANALYST;
