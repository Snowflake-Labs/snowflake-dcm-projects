/*=============================================================================
  03_schema_change.sql — Run AFTER the second DCM Deploy (with immutability)

  Refreshes the modified dynamic table and verifies the immutability backfill.
  Orders 1024 and 1025 were inserted with CURRENT_TIMESTAMP() in
  02_post_deploy.sql, so they fall within the mutable window and will be
  recomputed with the new PROFIT_MARGIN_PCT column.
=============================================================================*/

----------------------------------------------------------------------
-- 1. Refresh After Redeployment
----------------------------------------------------------------------
ALTER DYNAMIC TABLE DCM_DEMO_1_DEV.ANALYTICS.ENRICHED_ORDER_DETAILS REFRESH;

----------------------------------------------------------------------
-- 2. Verify — Immutable Rows (NULL) vs Mutable Rows (computed)
----------------------------------------------------------------------
SELECT
    ORDER_ID,
    ORDER_TS,
    MENU_ITEM_NAME,
    LINE_ITEM_REVENUE,
    PROFIT_MARGIN_PCT,
    metadata$is_immutable AS IS_IMMUTABLE
FROM DCM_DEMO_1_DEV.ANALYTICS.ENRICHED_ORDER_DETAILS
ORDER BY ORDER_TS DESC;
