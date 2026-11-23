/*=============================================================================
  04_marketing_post_deploy.sql: Run AFTER the Marketing project is deployed

  Runs the Marketing team's Spark pipeline once and reads what it wrote.
  Runs as the Marketing team role, which owns the Marketing project.
=============================================================================*/

USE ROLE DCM_DEMO_2_MARKETING_DEV_ADMIN;
USE WAREHOUSE DCM_DEMO_2_MARKETING_WH_DEV;

----------------------------------------------------------------------
-- 1. Run the task graph once
--
-- The graph deploys suspended, so its daily schedule never fires. EXECUTE
-- TASK GRAPH runs it on demand: CHECK_INPUTS, then BUILD_SEGMENTS (the Spark
-- job), then REPORT_SEGMENTS. ON_FAILURE is recorded as SKIPPED: it runs only
-- when a step fails. It runs asynchronously; the Spark job takes a minute or
-- two. Rerun the history query until all four tasks have a state.
----------------------------------------------------------------------
EXECUTE TASK GRAPH DCM_DEMO_2_MARKETING_DEV.SEGMENTS.SEGMENTS_PIPELINE;

SELECT NAME, STATE, RETURN_VALUE, ERROR_MESSAGE, SCHEDULED_TIME
FROM TABLE(DCM_DEMO_2_MARKETING_DEV.INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD(hour, -1, CURRENT_TIMESTAMP())))
WHERE DATABASE_NAME = 'DCM_DEMO_2_MARKETING_DEV'
ORDER BY SCHEDULED_TIME DESC;

----------------------------------------------------------------------
-- 2. Read the segments
--
-- The table is the one DCM created: an Iceberg table, same comment, same
-- created time. The Spark job only replaced its rows.
----------------------------------------------------------------------
SELECT * FROM DCM_DEMO_2_MARKETING_DEV.SEGMENTS.CUSTOMER_SEGMENTS ORDER BY TOTAL_VALUE DESC;

SELECT TABLE_NAME, IS_ICEBERG, COMMENT, CREATED, ROW_COUNT
FROM DCM_DEMO_2_MARKETING_DEV.INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = 'CUSTOMER_SEGMENTS';
