/*=============================================================================
  04_marketing_post_deploy.sql: Run AFTER the Marketing project is deployed

  Runs the Marketing team's Spark pipeline once, reads what it wrote, then
  checks that the two teams are separate. Runs as the Marketing team role,
  which owns the Marketing project.

  USE SECONDARY ROLES NONE makes every statement run with the team role alone.
  Without it, secondary roles granted to your user (often ACCOUNTADMIN) would
  add their privileges and every check in section 3 would pass.
=============================================================================*/

USE ROLE DCM_DEMO_2_MARKETING_DEV_ADMIN;
USE SECONDARY ROLES NONE;
USE WAREHOUSE DCM_DEMO_2_MARKETING_WH_DEV;

----------------------------------------------------------------------
-- 1. Run the task graph once
--
-- The graph deploys suspended, so its daily schedule never fires. EXECUTE
-- TASK GRAPH runs it on demand: CHECK_INPUTS, then BUILD_SEGMENTS (the Spark
-- job), then REPORT_SEGMENTS. It runs asynchronously; the Spark job takes a
-- minute or two. Rerun the history query until all three show SUCCEEDED.
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
-- The table is the one DCM created: same comment, same created time. The
-- Spark job only replaced its rows.
----------------------------------------------------------------------
SELECT * FROM DCM_DEMO_2_MARKETING_DEV.SEGMENTS.CUSTOMER_SEGMENTS ORDER BY TOTAL_VALUE DESC;

SELECT TABLE_NAME, COMMENT, CREATED, ROW_COUNT
FROM DCM_DEMO_2_MARKETING_DEV.INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = 'CUSTOMER_SEGMENTS';

----------------------------------------------------------------------
-- 3. The two teams are separate
--
-- Run each statement on its own. The first succeeds; the other two fail.
----------------------------------------------------------------------

-- Both teams read the shared raw layer.
SELECT COUNT(*) FROM DCM_DEMO_2_DEV.RAW.TRADE_STG;

-- Marketing cannot see the Finance database at all.
-- Expected: Database 'DCM_DEMO_2_FINANCE_DEV' does not exist or not authorized.
SELECT COUNT(*) FROM DCM_DEMO_2_FINANCE_DEV.GOLD.DAILY_TRADING;

-- No team writes to the platform's raw layer.
-- Expected: Insufficient privileges to operate on table 'TRADE_STG'.
INSERT INTO DCM_DEMO_2_DEV.RAW.TRADE_STG (T_ID) VALUES (9999);

----------------------------------------------------------------------
-- 4. The read-only role
--
-- Each team database has a READER database role for consumers: it reads
-- everything the team publishes, including tables created after the grant,
-- and changes nothing. The team admin role holds it and hands it out.
----------------------------------------------------------------------
SHOW GRANTS TO DATABASE ROLE DCM_DEMO_2_MARKETING_DEV.READER;
