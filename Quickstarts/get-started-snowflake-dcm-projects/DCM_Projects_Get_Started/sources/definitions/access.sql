-- ### Access and governance
--
-- The warehouse the pipeline runs on, a read-only role, and the one governance
-- example the guide needs: a tag declared here and attached to a table.

DEFINE WAREHOUSE DCM_DEMO_1_WH{{env_suffix}}
WITH
    WAREHOUSE_SIZE = '{{wh_size}}'
    AUTO_SUSPEND = 300
    COMMENT = 'Compute for the DCM Projects Quickstart demo';

DEFINE ROLE DCM_DEMO_1{{env_suffix}}_READ
    COMMENT = 'Read-only access to the demo database';

GRANT USAGE ON DATABASE DCM_DEMO_1{{env_suffix}}         TO ROLE DCM_DEMO_1{{env_suffix}}_READ;
GRANT USAGE ON SCHEMA DCM_DEMO_1{{env_suffix}}.RAW       TO ROLE DCM_DEMO_1{{env_suffix}}_READ;
GRANT USAGE ON SCHEMA DCM_DEMO_1{{env_suffix}}.ANALYTICS TO ROLE DCM_DEMO_1{{env_suffix}}_READ;
GRANT USAGE ON WAREHOUSE DCM_DEMO_1_WH{{env_suffix}}     TO ROLE DCM_DEMO_1{{env_suffix}}_READ;

-- One inherited grant covers current AND future objects of a type in a
-- container, which is why it is preferred over GRANT ON ALL or GRANT ON FUTURE:
-- those snapshot what exists at plan time and expand into one managed grant per
-- object. Inherited grants need the account-level opt-in set in
-- scripts/01_pre_deploy.sql, and cannot be combined with WITH GRANT OPTION,
-- CASCADE or RESTRICT.
GRANT INHERITED SELECT ON ALL TABLES IN DATABASE DCM_DEMO_1{{env_suffix}}         TO ROLE DCM_DEMO_1{{env_suffix}}_READ;
GRANT INHERITED SELECT ON ALL DYNAMIC TABLES IN DATABASE DCM_DEMO_1{{env_suffix}} TO ROLE DCM_DEMO_1{{env_suffix}}_READ;

-- Semantic views take an ordinary grant.
GRANT SELECT ON SEMANTIC VIEW DCM_DEMO_1{{env_suffix}}.ANALYTICS.ORDER_ANALYTICS TO ROLE DCM_DEMO_1{{env_suffix}}_READ;

DEFINE TAG DCM_DEMO_1{{env_suffix}}.RAW.DATA_SENSITIVITY
    ALLOWED_VALUES 'PUBLIC', 'INTERNAL', 'RESTRICTED'
    COMMENT = 'Sensitivity classification for governed objects';

-- ATTACH TAG is a standalone statement rather than part of a DEFINE, which is
-- why it can set a tag at all: CREATE OR ALTER cannot set tags or policies.
-- Tables, views and dynamic tables also accept COLUMN <name> to tag a single
-- column. Semantic views are not a supported target.
ATTACH TAG DCM_DEMO_1{{env_suffix}}.RAW.DATA_SENSITIVITY = 'INTERNAL'
    TO TABLE DCM_DEMO_1{{env_suffix}}.RAW.ORDER_HEADER;
