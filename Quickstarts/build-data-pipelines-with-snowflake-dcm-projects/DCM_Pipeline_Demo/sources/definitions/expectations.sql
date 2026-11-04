-- ### Data quality expectations on the silver layer
--
-- Three checks on TRADES_ENRICHED, the one table every gold number is built
-- from. Each attaches a system data metric function and names the condition
-- that must hold. Because the dynamic table sets
-- DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES', they are evaluated after each
-- refresh; EXECUTE DCM PROJECT ... TEST ALL evaluates them on demand.

-- Every trade must resolve to a customer. A NULL here means the trade's
-- account is missing from ACCOUNT_STG.
ATTACH DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT
    TO DYNAMIC TABLE DCM_DEMO_2_FINANCE{{env_suffix}}.SILVER.TRADES_ENRICHED
    ON (C_ID)
    EXPECTATION EVERY_TRADE_HAS_A_CUSTOMER (value = 0);

-- One row per trade. A duplicate means a join fanned out.
ATTACH DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT
    TO DYNAMIC TABLE DCM_DEMO_2_FINANCE{{env_suffix}}.SILVER.TRADES_ENRICHED
    ON (T_ID)
    EXPECTATION NO_DUPLICATE_TRADES (value = 0);

-- A completed trade always moves at least one share.
ATTACH DATA METRIC FUNCTION SNOWFLAKE.CORE.MIN
    TO DYNAMIC TABLE DCM_DEMO_2_FINANCE{{env_suffix}}.SILVER.TRADES_ENRICHED
    ON (T_QTY)
    EXPECTATION QUANTITY_IS_POSITIVE (value > 0);
