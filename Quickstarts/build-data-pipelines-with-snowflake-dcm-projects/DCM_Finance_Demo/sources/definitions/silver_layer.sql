-- ### Silver layer
--
-- One dynamic table that joins the three raw staging tables from the Platform
-- project into one row per trade, with the account and customer it belongs to.
--
-- The joins are LEFT joins on purpose. A trade whose account has not arrived
-- yet still shows up here, with an empty customer, instead of silently
-- disappearing. The expectation EVERY_TRADE_HAS_A_CUSTOMER in expectations.sql
-- is what turns that gap into a failing check.
--
-- TARGET_LAG = 'DOWNSTREAM' gives this table no schedule of its own: it
-- refreshes whenever a gold table needs it.

DEFINE SCHEMA DCM_DEMO_2_FINANCE{{env_suffix}}.SILVER
    COMMENT = 'Cleaned and joined trade data';

DEFINE DYNAMIC TABLE DCM_DEMO_2_FINANCE{{env_suffix}}.SILVER.TRADES_ENRICHED
    WAREHOUSE = DCM_DEMO_2_FINANCE_WH{{env_suffix}}
    TARGET_LAG = 'DOWNSTREAM'
    INITIALIZE = 'ON_SCHEDULE'
    DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES'
    COMMENT = 'One row per trade, joined to its account and customer'
AS
SELECT
    T.T_ID,
    T.T_DTS,
    T.T_S_SYMB,
    T.T_TT_ID,
    T.T_QTY,
    T.T_TRADE_PRICE,
    T.T_QTY * T.T_TRADE_PRICE AS TRADE_VALUE,
    T.T_CHRG,
    T.T_COMM,
    T.T_CA_ID,
    A.CA_NAME,
    C.C_ID,
    C.C_F_NAME || ' ' || C.C_L_NAME AS CUSTOMER_NAME,
    C.C_TIER
FROM DCM_DEMO_2{{env_suffix}}.RAW.TRADE_STG T
LEFT JOIN DCM_DEMO_2{{env_suffix}}.RAW.ACCOUNT_STG A
    ON T.T_CA_ID = A.CA_ID
LEFT JOIN DCM_DEMO_2{{env_suffix}}.RAW.CUSTOMER_STG C
    ON A.CA_C_ID = C.C_ID
WHERE T.T_ST_ID = 'CMPT';
