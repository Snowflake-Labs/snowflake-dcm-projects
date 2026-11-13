-- ### Data quality expectations on the silver layer
--
-- Three checks on TRADES_ENRICHED, the one table every gold number is built
-- from. Each attaches a system data metric function and names the condition
-- that must hold. Because the dynamic table sets
-- DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES', they are evaluated after each
-- refresh; EXECUTE DCM PROJECT ... TEST ALL evaluates them on demand.
--
-- All three checks are gates. ON_VIOLATION_ACTION =
-- (TYPE = 'SKIP_DOWNSTREAM_DYNAMIC_TABLE_REFRESH') means that while a check
-- fails, every dynamic table downstream of TRADES_ENRICHED skips its refresh
-- and keeps serving its last good contents. Nothing is suspended: the next
-- refresh after the checks pass again catches up on its own.
-- Gate every expectation on a monitored dynamic table, so that any failing
-- check holds the downstream refreshes.

-- Every trade must resolve to a customer. A NULL here means the trade's
-- account is missing from ACCOUNT_STG.
ATTACH DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT
    TO DYNAMIC TABLE DCM_DEMO_2_FINANCE{{env_suffix}}.SILVER.TRADES_ENRICHED
    ON (C_ID)
    EXPECTATION EVERY_TRADE_HAS_A_CUSTOMER (value = 0)
    ON_VIOLATION_ACTION = (TYPE = 'SKIP_DOWNSTREAM_DYNAMIC_TABLE_REFRESH');

-- One row per trade. A duplicate means a join fanned out.
ATTACH DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT
    TO DYNAMIC TABLE DCM_DEMO_2_FINANCE{{env_suffix}}.SILVER.TRADES_ENRICHED
    ON (T_ID)
    EXPECTATION NO_DUPLICATE_TRADES (value = 0)
    ON_VIOLATION_ACTION = (TYPE = 'SKIP_DOWNSTREAM_DYNAMIC_TABLE_REFRESH');

-- A completed trade always moves at least one share.
ATTACH DATA METRIC FUNCTION SNOWFLAKE.CORE.MIN
    TO DYNAMIC TABLE DCM_DEMO_2_FINANCE{{env_suffix}}.SILVER.TRADES_ENRICHED
    ON (T_QTY)
    EXPECTATION QUANTITY_IS_POSITIVE (value > 0)
    ON_VIOLATION_ACTION = (TYPE = 'SKIP_DOWNSTREAM_DYNAMIC_TABLE_REFRESH');

-- ### Data quality expectations on the gold layer
--
-- The gold tables are what the semantic view, the agent and dashboards read,
-- so they get their own checks. They are gated the same way as silver. Today
-- nothing is downstream of gold, so a failing check here holds no refresh; the
-- gate starts working as soon as a dynamic table is built on top of gold.

-- One row per trading day.
ATTACH DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT
    TO DYNAMIC TABLE DCM_DEMO_2_FINANCE{{env_suffix}}.GOLD.DAILY_TRADING
    ON (TRADE_DATE)
    EXPECTATION ONE_ROW_PER_DAY (value = 0)
    ON_VIOLATION_ACTION = (TYPE = 'SKIP_DOWNSTREAM_DYNAMIC_TABLE_REFRESH');

-- Every customer row carries a name for the agent and dashboards to show.
ATTACH DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT
    TO DYNAMIC TABLE DCM_DEMO_2_FINANCE{{env_suffix}}.GOLD.CUSTOMER_TRADING
    ON (CUSTOMER_NAME)
    EXPECTATION EVERY_CUSTOMER_IS_NAMED (value = 0)
    ON_VIOLATION_ACTION = (TYPE = 'SKIP_DOWNSTREAM_DYNAMIC_TABLE_REFRESH');
