/*=============================================================================
  03_verify_evolution.sql: Run AFTER the second DCM Deploy (frozen region +
  AI column), as DCM_DEVELOPER

  Section 1 inserts orders 1024 and 1025 with CURRENT_TIMESTAMP(), so they
  fall in the active region whenever you run this script and get the new
  DIET_CLASSIFICATION column. AI_CLASSIFY only runs on the active region;
  the October 2023 rows are frozen and skipped.
=============================================================================*/

USE ROLE DCM_DEVELOPER;
USE WAREHOUSE DCM_DEMO_3_WH_DEV;

----------------------------------------------------------------------
-- 1. Insert two new orders, stamped now
----------------------------------------------------------------------
INSERT INTO DCM_DEMO_3_DEV.RAW.ORDERS VALUES
    (1024, CURRENT_TIMESTAMP(), 2, 'Taco Titan', 7, 3),
    (1025, CURRENT_TIMESTAMP(), 3, 'The Rolling Dough', 8, 1);

----------------------------------------------------------------------
-- 2. Refresh after the redeployment
----------------------------------------------------------------------
ALTER DYNAMIC TABLE DCM_DEMO_3_DEV.ANALYTICS.ENRICHED_ORDER_DETAILS REFRESH;

----------------------------------------------------------------------
-- 3. Verify: frozen rows (NULL) vs active rows (AI-classified)
----------------------------------------------------------------------
SELECT
    ORDER_ID,
    ORDER_TS,
    MENU_ITEM_NAME,
    LINE_ITEM_REVENUE,
    DIET_CLASSIFICATION,
    METADATA$IS_FROZEN AS IS_FROZEN
FROM DCM_DEMO_3_DEV.ANALYTICS.ENRICHED_ORDER_DETAILS
ORDER BY ORDER_TS DESC, ORDER_ID;

----------------------------------------------------------------------
-- 4. What the refresh recomputed
--
-- ROWS_INSERTED counts the rows the refresh computed (and ran AI_CLASSIFY
-- on). ROWS_DELETED and ROWS_COPIED stay at 0 after the change: the frozen
-- rows were neither rewritten nor copied.
----------------------------------------------------------------------
SELECT
    REFRESH_START_TIME,
    REFRESH_ACTION,
    STATISTICS:"numInsertedRows"::INT AS ROWS_INSERTED,
    STATISTICS:"numDeletedRows"::INT AS ROWS_DELETED,
    STATISTICS:"numCopiedRows"::INT AS ROWS_COPIED
FROM TABLE(DCM_DEMO_3_DEV.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
    NAME => 'DCM_DEMO_3_DEV.ANALYTICS.ENRICHED_ORDER_DETAILS'))
ORDER BY REFRESH_START_TIME DESC;

----------------------------------------------------------------------
-- 5. The serve layer carries the new column
--
-- The same deploy changed V_DASHBOARD_SALES_BY_CATEGORY_CITY and the
-- SALES_ANALYTICS semantic view, so DIET_CLASSIFICATION reaches dashboards
-- and Cortex Analyst. Frozen rows show as NULL.
----------------------------------------------------------------------
SELECT * FROM SEMANTIC_VIEW(
    DCM_DEMO_3_DEV.SERVE.SALES_ANALYTICS
    DIMENSIONS order_lines.DIET_CLASSIFICATION
    METRICS order_lines.TOTAL_REVENUE, order_lines.ORDER_COUNT
)
ORDER BY DIET_CLASSIFICATION;
