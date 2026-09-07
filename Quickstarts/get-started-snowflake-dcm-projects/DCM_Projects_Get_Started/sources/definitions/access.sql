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

GRANT USAGE on database DCM_DEMO_1{{env_suffix}}         to role DCM_DEMO_1{{env_suffix}}_READ;
GRANT USAGE on schema DCM_DEMO_1{{env_suffix}}.RAW       to role DCM_DEMO_1{{env_suffix}}_READ;
GRANT USAGE on schema DCM_DEMO_1{{env_suffix}}.ANALYTICS to role DCM_DEMO_1{{env_suffix}}_READ;
GRANT USAGE on schema DCM_DEMO_1{{env_suffix}}.SERVE     to role DCM_DEMO_1{{env_suffix}}_READ;
GRANT USAGE on warehouse DCM_DEMO_1_WH{{env_suffix}}     to role DCM_DEMO_1{{env_suffix}}_READ;

-- One inherited grant covers current AND future objects of a type in a
-- container, which is why it is preferred over GRANT ON ALL or GRANT ON FUTURE:
-- those snapshot what exists at plan time and expand into one managed grant per
-- object. Inherited grants need the account-level opt-in set in
-- scripts/01_pre_deploy.sql, and cannot be combined with WITH GRANT OPTION,
-- CASCADE or RESTRICT.
GRANT INHERITED SELECT on all tables in database DCM_DEMO_1{{env_suffix}}         to role DCM_DEMO_1{{env_suffix}}_READ;
GRANT INHERITED SELECT on all dynamic tables in database DCM_DEMO_1{{env_suffix}} to role DCM_DEMO_1{{env_suffix}}_READ;

-- Semantic views take an ordinary grant.
GRANT SELECT on semantic view DCM_DEMO_1{{env_suffix}}.SERVE.ORDER_ANALYTICS     to role DCM_DEMO_1{{env_suffix}}_READ;

DEFINE TAG DCM_DEMO_1{{env_suffix}}.RAW.DATA_SENSITIVITY
    ALLOWED_VALUES 'PUBLIC', 'INTERNAL', 'RESTRICTED'
    COMMENT = 'Sensitivity classification for governed objects';

-- ATTACH TAG is a standalone statement rather than part of a DEFINE, which is
-- why it can set a tag at all: CREATE OR ALTER cannot set tags or policies.
-- It applies to whole objects only. Tables and dynamic tables are valid
-- targets; views, semantic views and individual columns are not.
ATTACH TAG DCM_DEMO_1{{env_suffix}}.RAW.DATA_SENSITIVITY = 'INTERNAL'
    TO TABLE DCM_DEMO_1{{env_suffix}}.RAW.ORDER_HEADER;
