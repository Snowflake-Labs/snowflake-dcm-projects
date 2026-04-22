/*=============================================================================
  02_post_deploy.sql — Run AFTER the first DCM Deploy succeeds

  Streams and alerts are not yet supported as DCM `DEFINE` statements,
  so we create them here — then seed some source rows and trigger the
  root task. The DMF attachments are defined natively inside the DCM
  Project (see `sources/definitions/expectations.sql`).

  Replace <env_suffix> with the suffix from your manifest target
  (e.g. `_DEV` for DCM_DEV). All object names below use `_dev` for the
  default DEV target.
=============================================================================*/

USE ROLE dcm_developer;
USE WAREHOUSE dcm_wh;

----------------------------------------------------------------------
-- 1. Create the stream on TASK_DEMO_TABLE (used by DEMO_TASK_8)
----------------------------------------------------------------------
CREATE OR REPLACE STREAM dcm_demo_4_dev.pipeline.demo_stream
    ON TABLE dcm_demo_4_dev.pipeline.task_demo_table
    COMMENT = 'Empty stream — DEMO_TASK_8 will be skipped unless this has data';

----------------------------------------------------------------------
-- 2. Serverless alert for any failed Task in our database
----------------------------------------------------------------------
CREATE OR REPLACE ALERT dcm_demo_4_dev.pipeline.failed_task_alert
    SCHEDULE = '60 MINUTE'
    IF (EXISTS (
        SELECT NAME, SCHEMA_NAME
        FROM TABLE(DCM_DEMO_4_DEV.INFORMATION_SCHEMA.TASK_HISTORY(
            SCHEDULED_TIME_RANGE_START => (GREATEST(
                TIMEADD('DAY', -7, CURRENT_TIMESTAMP),
                SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME())),
            SCHEDULED_TIME_RANGE_END   => SNOWFLAKE.ALERT.SCHEDULED_TIME(),
            ERROR_ONLY                 => TRUE))))
    THEN
        BEGIN
            LET task_names STRING := (
                SELECT LISTAGG(DISTINCT(SCHEMA_NAME || '.' || NAME), ', ')
                FROM TABLE(RESULT_SCAN(SNOWFLAKE.ALERT.GET_CONDITION_QUERY_UUID())));

            CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
                SNOWFLAKE.NOTIFICATION.TEXT_HTML(
                    'Failed tasks detected: <b>' || :task_names || '</b>'),
                SNOWFLAKE.NOTIFICATION.EMAIL_INTEGRATION_CONFIG(
                    'dcm_demo_email_notifications',
                    'DCM Pipeline — Failed Task Alert',
                    ARRAY_CONSTRUCT('INSERT_YOUR_EMAIL'),   -- <-- Replace with your verified email
                    NULL, NULL));
        END;

ALTER ALERT dcm_demo_4_dev.pipeline.failed_task_alert RESUME;

----------------------------------------------------------------------
-- 3. Seed the source table so LOAD_RAW_DATA has rows to pull
----------------------------------------------------------------------
INSERT INTO dcm_demo_4_dev.pipeline.weather_data_source (DS, ZIPCODE, MIN_TEMP_IN_F, AVG_TEMP_IN_F, MAX_TEMP_IN_F)
VALUES
    ('2025-06-01', '94105', 55, 68, 80),
    ('2025-06-01', '10001', 60, 72, 85),
    ('2025-06-01', '60601', 50, 65, 78),
    ('2025-06-02', '94105', 57, 70, 82),
    ('2025-06-02', '10001', 62, 74, 87),
    ('2025-06-02', '60601', 52, 67, 79),
    ('2025-06-03', '94105', 58, 71, 83),
    ('2025-06-03', '10001', 63, 75, 88),
    ('2025-06-03', '60601', 53, 68, 80),
    ('2025-06-04', '94105', 59, 72, 84);

----------------------------------------------------------------------
-- 4. Kick off a manual run of the task graph
----------------------------------------------------------------------
EXECUTE TASK dcm_demo_4_dev.pipeline.demo_task_1;

----------------------------------------------------------------------
-- 5. Force-run the alert (don't wait 60 minutes for the schedule)
----------------------------------------------------------------------
EXECUTE ALERT dcm_demo_4_dev.pipeline.failed_task_alert;

----------------------------------------------------------------------
-- 6. Inspect
----------------------------------------------------------------------
-- Navigate to Monitoring → Task History in Snowsight for the graph view,
-- or query the task history programmatically:
SELECT NAME, STATE, RETURN_VALUE, ERROR_MESSAGE, QUERY_START_TIME
FROM TABLE(DCM_DEMO_4_DEV.INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('MINUTE', -30, CURRENT_TIMESTAMP())))
ORDER BY QUERY_START_TIME;
