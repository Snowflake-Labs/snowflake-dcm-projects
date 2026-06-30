/*=============================================================================
  03_verify_evolution.sql — Run AFTER the second DCM Deploy (with frozen region)

  Refreshes the modified dynamic table and verifies the frozen-region behavior.
  Orders 1024 and 1025 were inserted with CURRENT_TIMESTAMP() in
  02_post_deploy.sql, so they fall in the active region and will be recomputed
  with the new DIET_CLASSIFICATION column (the AI_CLASSIFY call only runs on the
  active region — frozen historical rows are skipped entirely).
=============================================================================*/

----------------------------------------------------------------------
-- 1. Refresh After Redeployment
----------------------------------------------------------------------
ALTER DYNAMIC TABLE DCM_DEMO_3_DEV.ANALYTICS.ENRICHED_ORDER_DETAILS REFRESH;

----------------------------------------------------------------------
-- 2. Verify — Frozen Rows (NULL) vs Active Rows (AI-classified)
----------------------------------------------------------------------
SELECT
    ORDER_ID,
    ORDER_TS,
    MENU_ITEM_NAME,
    LINE_ITEM_REVENUE,
    DIET_CLASSIFICATION,
    METADATA$IS_FROZEN AS IS_FROZEN
FROM DCM_DEMO_3_DEV.ANALYTICS.ENRICHED_ORDER_DETAILS
ORDER BY ORDER_TS DESC;
