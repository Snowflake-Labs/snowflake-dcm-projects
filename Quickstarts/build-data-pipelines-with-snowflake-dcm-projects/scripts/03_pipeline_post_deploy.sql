/*=============================================================================
  03_pipeline_post_deploy.sql: Run AFTER the Pipeline project is deployed

  Refreshes the pipeline, reads the gold layer and the semantic view, runs the
  data quality expectations, then fixes the problem they find. Runs as the
  Finance team role, which owns the Pipeline project.
=============================================================================*/

USE ROLE DCM_DEMO_2_FINANCE_DEV_ADMIN;
USE WAREHOUSE DCM_DEMO_2_FINANCE_WH_DEV;

----------------------------------------------------------------------
-- 1. Refresh the pipeline
--
-- The dynamic tables were created with INITIALIZE = 'ON_SCHEDULE', so they are
-- empty until their first refresh. Refreshing the two gold tables in one
-- statement refreshes the silver table they read first, once, and lands all
-- three on the same data timestamp.
----------------------------------------------------------------------
ALTER DYNAMIC TABLE DCM_DEMO_2_FINANCE_DEV.GOLD.DAILY_TRADING,
                    DCM_DEMO_2_FINANCE_DEV.GOLD.CUSTOMER_TRADING REFRESH;

----------------------------------------------------------------------
-- 2. Read the gold layer
----------------------------------------------------------------------
SELECT * FROM DCM_DEMO_2_FINANCE_DEV.GOLD.CUSTOMER_TRADING ORDER BY TOTAL_VALUE DESC;

----------------------------------------------------------------------
-- 3. Ask the semantic view
--
-- The same numbers, asked by metric and dimension name rather than by column.
----------------------------------------------------------------------
SELECT * FROM SEMANTIC_VIEW(
    DCM_DEMO_2_FINANCE_DEV.GOLD.TRADING_ANALYTICS
    METRICS daily.TOTAL_TRADES, daily.TOTAL_TRADE_VALUE
    DIMENSIONS daily.TRADE_DATE
)
ORDER BY TRADE_DATE;

----------------------------------------------------------------------
-- 4. Run every expectation the project defines
----------------------------------------------------------------------
EXECUTE DCM PROJECT DCM_DEMO_2_FINANCE_DEV.PROJECTS.FINANCE_PIPELINE TEST ALL;

----------------------------------------------------------------------
-- 5. Find the trade that failed
----------------------------------------------------------------------
SELECT T_ID, T_DTS, T_CA_ID, C_ID, CUSTOMER_NAME
FROM DCM_DEMO_2_FINANCE_DEV.SILVER.TRADES_ENRICHED
WHERE C_ID IS NULL;

----------------------------------------------------------------------
-- 6. Fix it: the missing account arrives
--
-- The raw tables belong to the platform, so the late account is loaded as
-- DCM_DEVELOPER, the platform's owner role. The Finance role can read RAW but
-- not write it.
----------------------------------------------------------------------
USE ROLE DCM_DEVELOPER;
INSERT INTO DCM_DEMO_2_DEV.RAW.ACCOUNT_STG (CA_ID, CA_C_ID, CA_NAME, CA_TAX_ST, CA_ST_ID)
VALUES (1009, 106, 'Mehta Retirement', 1, 'ACTV');

USE ROLE DCM_DEMO_2_FINANCE_DEV_ADMIN;
ALTER DYNAMIC TABLE DCM_DEMO_2_FINANCE_DEV.GOLD.DAILY_TRADING,
                    DCM_DEMO_2_FINANCE_DEV.GOLD.CUSTOMER_TRADING REFRESH;

EXECUTE DCM PROJECT DCM_DEMO_2_FINANCE_DEV.PROJECTS.FINANCE_PIPELINE TEST ALL;

----------------------------------------------------------------------
-- 7. Take note of this table
--
-- The newest trades first, and which of them sit in a frozen region. Nothing is
-- frozen yet.
----------------------------------------------------------------------
SELECT T_ID, T_DTS, TRADE_VALUE, METADATA$IS_FROZEN AS IS_FROZEN
FROM DCM_DEMO_2_FINANCE_DEV.SILVER.TRADES_ENRICHED
ORDER BY T_DTS DESC
LIMIT 4;
