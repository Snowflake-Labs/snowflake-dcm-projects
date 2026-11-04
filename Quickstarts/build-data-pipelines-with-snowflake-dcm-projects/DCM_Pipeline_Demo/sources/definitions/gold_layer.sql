-- ### Gold layer
--
-- Two aggregates that dashboards and the semantic view read. They hold the
-- pipeline's only real TARGET_LAG: the silver table upstream is DOWNSTREAM, so
-- this one hour is what drives the whole graph.

DEFINE SCHEMA DCM_DEMO_2_FINANCE{{env_suffix}}.GOLD
    COMMENT = 'Aggregates and the semantic view the business reads';

DEFINE DYNAMIC TABLE DCM_DEMO_2_FINANCE{{env_suffix}}.GOLD.DAILY_TRADING
    WAREHOUSE = DCM_DEMO_2_FINANCE_WH{{env_suffix}}
    TARGET_LAG = '1 hour'
    INITIALIZE = 'ON_SCHEDULE'
    COMMENT = 'Trading volume and value per day'
AS
SELECT
    T_DTS::DATE         AS TRADE_DATE,
    COUNT(*)            AS TRADE_COUNT,
    SUM(T_QTY)          AS TOTAL_QUANTITY,
    SUM(TRADE_VALUE)    AS TOTAL_VALUE
FROM DCM_DEMO_2_FINANCE{{env_suffix}}.SILVER.TRADES_ENRICHED
GROUP BY TRADE_DATE;

DEFINE DYNAMIC TABLE DCM_DEMO_2_FINANCE{{env_suffix}}.GOLD.CUSTOMER_TRADING
    WAREHOUSE = DCM_DEMO_2_FINANCE_WH{{env_suffix}}
    TARGET_LAG = '1 hour'
    INITIALIZE = 'ON_SCHEDULE'
    COMMENT = 'Trading activity per customer'
AS
SELECT
    C_ID,
    CUSTOMER_NAME,
    C_TIER,
    COUNT(*)            AS TRADE_COUNT,
    SUM(TRADE_VALUE)    AS TOTAL_VALUE,
    MAX(T_DTS)          AS LAST_TRADE_TS
FROM DCM_DEMO_2_FINANCE{{env_suffix}}.SILVER.TRADES_ENRICHED
GROUP BY C_ID, CUSTOMER_NAME, C_TIER;
