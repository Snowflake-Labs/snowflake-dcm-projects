/*=============================================================================
  04_schema_change.sql: Run AFTER the second Pipeline deploy

  The second deploy added a frozen region and a NET_VALUE column to
  SILVER.TRADES_ENRICHED. One new trade, one refresh, then one query that
  shows both effects.
=============================================================================*/

----------------------------------------------------------------------
-- 1. A trade placed right now
--
-- Stamped with CURRENT_TIMESTAMP(), so it is in the active region however
-- long ago you loaded the sample data. Runs as DCM_DEVELOPER, which owns the
-- raw tables.
----------------------------------------------------------------------
USE ROLE DCM_DEVELOPER;
USE WAREHOUSE DCM_DEMO_2_PLATFORM_WH_DEV;

INSERT INTO DCM_DEMO_2_DEV.RAW.TRADE_STG
    (T_ID, T_DTS, T_ST_ID, T_TT_ID, T_S_SYMB, T_QTY, T_TRADE_PRICE, T_CHRG, T_COMM, T_CA_ID)
VALUES
    (2023, CURRENT_TIMESTAMP(), 'CMPT', 'TMB', 'SNOW', 25, 152.40, 4.95, 2.50, 1002);

----------------------------------------------------------------------
-- 2. Refresh, as the Finance team role that owns the pipeline
----------------------------------------------------------------------
USE ROLE DCM_DEMO_2_FINANCE_DEV_ADMIN;
USE WAREHOUSE DCM_DEMO_2_FINANCE_WH_DEV;

ALTER DYNAMIC TABLE DCM_DEMO_2_FINANCE_DEV.GOLD.DAILY_TRADING,
                    DCM_DEMO_2_FINANCE_DEV.GOLD.CUSTOMER_TRADING REFRESH;

----------------------------------------------------------------------
-- 3. Verify
--
-- Trades from the last day are in the active region and get NET_VALUE
-- computed. Older trades are frozen: the refresh skipped them, so they keep
-- NULL.
----------------------------------------------------------------------
SELECT T_ID, T_DTS, TRADE_VALUE, NET_VALUE, METADATA$IS_FROZEN AS IS_FROZEN
FROM DCM_DEMO_2_FINANCE_DEV.SILVER.TRADES_ENRICHED
ORDER BY T_DTS DESC
LIMIT 5;

-- The frozen region is visible in the dynamic table's metadata too.
SHOW DYNAMIC TABLES LIKE 'TRADES_ENRICHED' IN SCHEMA DCM_DEMO_2_FINANCE_DEV.SILVER;
SELECT "name", "frozen_where" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
