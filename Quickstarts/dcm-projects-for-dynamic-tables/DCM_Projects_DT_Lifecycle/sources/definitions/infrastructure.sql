DEFINE WAREHOUSE DCM_DEMO_3_WH{{env_suffix}}
WITH
    WAREHOUSE_SIZE = '{{wh_size}}'
    AUTO_SUSPEND = 60
    COMMENT = 'Refreshes the dynamic tables of the DCM Projects for Dynamic Tables quickstart';

-- LOG_EVENT_LEVEL = ERROR makes every dynamic table in the database log its
-- refresh failures to the event table, which is what the DT_REFRESH_FAILURE
-- alert in alerts.sql reads.
DEFINE DATABASE DCM_DEMO_3{{env_suffix}}
    LOG_EVENT_LEVEL = ERROR
    COMMENT = 'This is a Quickstart Demo for DCM Projects with Dynamic Tables';

DEFINE SCHEMA DCM_DEMO_3{{env_suffix}}.PUBLIC
    COMMENT = 'Default schema, declared so DCM manages it';
