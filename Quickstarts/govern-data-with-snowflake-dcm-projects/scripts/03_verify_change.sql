/*=============================================================================
  03_verify_change.sql: Run AFTER the second deployment, the one that widens
  the row access policy to include UK customers.

  Nothing to refresh: a row access policy is evaluated at query time, so the
  next query as the analyst role already sees the change.
=============================================================================*/

-- 1. The analyst now sees USA and UK rows, emails still masked
USE ROLE DCM_DEMO_5_DEV_ANALYST;
USE WAREHOUSE DCM_DEMO_5_WH_DEV;
SELECT CUSTOMER_ID, FIRST_NAME, EMAIL, COUNTRY FROM DCM_DEMO_5_DEV.RAW.CUSTOMER ORDER BY CUSTOMER_ID;

-- 2. The policy body DCM deployed
USE ROLE DCM_DEVELOPER;
USE WAREHOUSE DCM_DEMO_5_WH_DEV;
DESCRIBE ROW ACCESS POLICY DCM_DEMO_5_DEV.GOV.CUSTOMER_COUNTRY_FILTER;
