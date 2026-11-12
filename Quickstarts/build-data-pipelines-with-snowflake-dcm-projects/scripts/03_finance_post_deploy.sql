/*=============================================================================
  03_finance_post_deploy.sql: Run AFTER the Finance project is deployed

  The Finance team's pipeline end to end: start the alert, refresh the pipeline,
  read the gold layer, run the data quality checks, see how the checks gate
  downstream refreshes, and ask the agent. Runs as the Finance team role, which
  owns the Finance project.
=============================================================================*/

USE ROLE DCM_DEMO_2_FINANCE_DEV_ADMIN;
USE WAREHOUSE DCM_DEMO_2_FINANCE_WH_DEV;

----------------------------------------------------------------------
-- 1. Start the alert
--
-- A template alert deploys suspended. Resume it so it checks the silver
-- layer's expectations every five minutes and notifies you in Snowsight (the
-- bell icon) if one fails.
----------------------------------------------------------------------
ALTER ALERT DCM_DEMO_2_FINANCE_DEV.SILVER.EXPECTATION_VIOLATIONS RESUME;

----------------------------------------------------------------------
-- 2. Refresh the pipeline
--
-- The dynamic tables were created with INITIALIZE = 'ON_SCHEDULE', so they are
-- empty until their first refresh. Refreshing the two gold tables in one
-- statement refreshes the silver table they read first, once, and lands all
-- three on the same data timestamp.
----------------------------------------------------------------------
ALTER DYNAMIC TABLE DCM_DEMO_2_FINANCE_DEV.GOLD.DAILY_TRADING,
                    DCM_DEMO_2_FINANCE_DEV.GOLD.CUSTOMER_TRADING REFRESH;

----------------------------------------------------------------------
-- 3. Read the gold layer and the semantic view
----------------------------------------------------------------------
SELECT * FROM DCM_DEMO_2_FINANCE_DEV.GOLD.CUSTOMER_TRADING ORDER BY TOTAL_VALUE DESC;

SELECT * FROM SEMANTIC_VIEW(
    DCM_DEMO_2_FINANCE_DEV.GOLD.TRADING_ANALYTICS
    METRICS daily.TOTAL_TRADES, daily.TOTAL_TRADE_VALUE
    DIMENSIONS daily.TRADE_DATE
)
ORDER BY TRADE_DATE;

----------------------------------------------------------------------
-- 4. Run every expectation the project defines
--
-- All three pass on the sample data.
----------------------------------------------------------------------
EXECUTE DCM PROJECT DCM_DEMO_2_FINANCE_DEV.PROJECTS.FINANCE_PIPELINE TEST ALL;

----------------------------------------------------------------------
-- 5. The data quality gates
--
-- Each expectation carries ON_VIOLATION_ACTION =
-- (TYPE = 'SKIP_DOWNSTREAM_DYNAMIC_TABLE_REFRESH'). While one fails, every
-- dynamic table downstream of TRADES_ENRICHED skips its refresh and keeps its
-- last contents; the next refresh after the check passes catches up. A skipped
-- refresh shows STATE_CODE UPSTREAM_DATA_QUALITY_VIOLATION in the refresh
-- history. LAST_TRIGGER_STATUS reads NOT_TRIGGERED while the checks pass.
----------------------------------------------------------------------
SELECT METRIC_NAME,
       PARSE_JSON(REF_ARGUMENTS)[0]:name::STRING   AS COLUMN_NAME,
       VIOLATION_ACTION:type::STRING                AS ACTION,
       VIOLATION_ACTION:last_trigger_status::STRING AS LAST_TRIGGER_STATUS
FROM TABLE(DCM_DEMO_2_FINANCE_DEV.INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
    REF_ENTITY_NAME   => 'DCM_DEMO_2_FINANCE_DEV.SILVER.TRADES_ENRICHED',
    REF_ENTITY_DOMAIN => 'DYNAMIC TABLE'));

SELECT NAME, STATE, STATE_CODE, DATA_TIMESTAMP
FROM TABLE(DCM_DEMO_2_FINANCE_DEV.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
    NAME_PREFIX => 'DCM_DEMO_2_FINANCE_DEV.'))
ORDER BY DATA_TIMESTAMP DESC
LIMIT 3;

----------------------------------------------------------------------
-- 6. Ask the agent
--
-- In Snowsight: AI & ML > Agents > Trading Assistant, and ask
-- "Which customers traded the most by value?". Or from SQL:
----------------------------------------------------------------------
SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
    'DCM_DEMO_2_FINANCE_DEV.GOLD.TRADING_AGENT',
    '{"messages": [{"role": "user", "content": [{"type": "text", "text": "Which customers traded the most by value?"}]}]}'
) AS RESPONSE;
