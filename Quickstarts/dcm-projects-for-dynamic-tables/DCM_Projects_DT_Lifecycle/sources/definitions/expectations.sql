-- Expectations need two account-level grants to the project owner, made in
-- scripts/01_pre_deploy.sql: EXECUTE DATA METRIC FUNCTION ON ACCOUNT and the
-- SNOWFLAKE.DATA_QUALITY_MONITORING_VIEWER application role.
-- Every table, dynamic table or view with an expectation also needs a
-- DATA_METRIC_SCHEDULE in its definition.

-- System DMF on a table column
ATTACH DATA METRIC FUNCTION SNOWFLAKE.CORE.MIN
    TO TABLE DCM_DEMO_3{{env_suffix}}.RAW.ORDERS
    ON (QUANTITY)
    EXPECTATION POSITIVE_QUANTITY (value > 0);

-- System DMF on a dynamic table column
ATTACH DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT
    TO DYNAMIC TABLE DCM_DEMO_3{{env_suffix}}.ANALYTICS.ENRICHED_ORDER_DETAILS
    ON (CUSTOMER_CITY)
    EXPECTATION NO_MISSING_CITIES (value = 0);

-- System DMF on a view column
ATTACH DATA METRIC FUNCTION SNOWFLAKE.CORE.MIN
    TO VIEW DCM_DEMO_3{{env_suffix}}.SERVE.V_DASHBOARD_DAILY_SALES
    ON (DAILY_ORDERS)
    EXPECTATION ORDERS_POSITIVE (value >= 0);
