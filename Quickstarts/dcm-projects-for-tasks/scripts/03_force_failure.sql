/*=============================================================================
  03_force_failure.sql: run AFTER 02_post_deploy.sql

  Sends one broken reading through the graph so BUILD_DAILY_SUMMARY fails,
  runs the TASK_ERROR_RATE alert immediately instead of waiting for its hourly
  schedule, shows that it fired and sent its email, then removes the broken
  reading. Takes about four minutes, most of it waiting.
=============================================================================*/

USE ROLE DCM_DEVELOPER;
USE WAREHOUSE DCM_DEMO_4_WH_DEV;

----------------------------------------------------------------------
-- 1. A sensor glitch: an average temperature of one billion degrees
--
-- DAILY_WEATHER_SUMMARY stores AVG_TEMP_IN_F as NUMBER(10,1), which cannot
-- hold it, so BUILD_DAILY_SUMMARY fails with a numeric overflow.
----------------------------------------------------------------------
INSERT INTO DCM_DEMO_4_DEV.PIPELINE.RAW_WEATHER_DATA (DS, ZIPCODE, MIN_TEMP_IN_F, AVG_TEMP_IN_F, MAX_TEMP_IN_F)
VALUES ('2026-06-02'::DATE, '99999', 50, 1000000000, 80);

----------------------------------------------------------------------
-- 2. Run the graph and wait for the run and its events
--
-- Task run events reach the event table a minute or two after the run ends,
-- so the script waits before it runs the alert.
----------------------------------------------------------------------
EXECUTE TASK GRAPH DCM_DEMO_4_DEV.PIPELINE.WEATHER_PIPELINE;
CALL SYSTEM$WAIT(180);

SHOW TASK GRAPHS IN SCHEMA DCM_DEMO_4_DEV.PIPELINE;
SET GRAPH_ID = (SELECT "id" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));

SELECT GRAPH_VERSION, NAME, STATE, RETURN_VALUE, ERROR_MESSAGE
FROM TABLE(DCM_DEMO_4_DEV.INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('HOUR', -1, CURRENT_TIMESTAMP()),
    GRAPH_ID => $GRAPH_ID,
    RESULT_LIMIT => 10000))
WHERE STATE <> 'SCHEDULED'
QUALIFY RUN_ID = MAX(RUN_ID) OVER ()
ORDER BY SCHEDULED_TIME, NAME;

----------------------------------------------------------------------
-- 3. Measure the error rate, then run the alert now and check that it fired
--
-- The same count the TASKS_ERROR_RATE template makes on its first run: task
-- status events in this database over the last hour. Every status event
-- counts in the denominator, skipped runs included.
----------------------------------------------------------------------
SELECT COUNT_IF(VALUE:state::STRING = 'FAILED') AS FAILED_EVENTS,
       COUNT(*) AS ALL_STATUS_EVENTS,
       ROUND(FAILED_EVENTS / ALL_STATUS_EVENTS, 3) AS ERROR_RATE
FROM SNOWFLAKE.TELEMETRY.EVENTS_VIEW
WHERE RECORD_TYPE = 'EVENT'
  AND RECORD:name::STRING = 'execution.status'
  AND RESOURCE_ATTRIBUTES:"snow.executable.type"::STRING = 'TASK'
  AND RESOURCE_ATTRIBUTES:"snow.database.name"::STRING = 'DCM_DEMO_4_DEV'
  AND TIMESTAMP > DATEADD('HOUR', -1, SYSDATE());   -- event TIMESTAMP is UTC

EXECUTE ALERT DCM_DEMO_4_DEV.PIPELINE.TASK_ERROR_RATE;
CALL SYSTEM$WAIT(30);

SELECT NAME, STATE, SCHEDULED_TIME, SQL_ERROR_MESSAGE
FROM TABLE(DCM_DEMO_4_DEV.INFORMATION_SCHEMA.ALERT_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('HOUR', -1, CURRENT_TIMESTAMP())))
WHERE NAME = 'TASK_ERROR_RATE' AND STATE <> 'SCHEDULED'
ORDER BY SCHEDULED_TIME DESC;

SELECT INTEGRATION_NAME, STATUS, CREATED, ERROR_MESSAGE
FROM TABLE(DCM_DEMO_4_DEV.INFORMATION_SCHEMA.NOTIFICATION_HISTORY(
    START_TIME => DATEADD('HOUR', -1, CURRENT_TIMESTAMP()),
    INTEGRATION_NAME => 'DCM_DEMO_4_EMAIL_INT'))
ORDER BY CREATED DESC;

----------------------------------------------------------------------
-- 4. Remove the broken reading so the next run succeeds again
----------------------------------------------------------------------
DELETE FROM DCM_DEMO_4_DEV.PIPELINE.RAW_WEATHER_DATA WHERE ZIPCODE = '99999';
