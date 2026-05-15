/*=============================================================================
  02_post_deploy.sql — Run AFTER the first DCM Deploy succeeds

  Streams are not yet supported as DCM `DEFINE` statements, so we create the
  stream here. We also resume the DCM-managed failed-task alert (which deploys
  suspended), seed some source rows, and trigger the root task. The DMF
  attachments and failed-task alert itself are already DCM-managed (see
  `sources/definitions/expectations.sql` and `sources/definitions/alerts.sql`).

  Replace <env_suffix> with the suffix from your manifest target
  (e.g. `_DEV` for DCM_DEV). All object names below use `_dev` for the
  default DEV target.
=============================================================================*/

USE ROLE dcm_developer;

----------------------------------------------------------------------
-- 1. Create the stream on TASK_DEMO_TABLE (used by DEMO_TASK_8)
----------------------------------------------------------------------
CREATE OR REPLACE STREAM dcm_demo_4_dev.pipeline.demo_stream
    ON TABLE dcm_demo_4_dev.pipeline.task_demo_table
    COMMENT = 'Empty stream — DEMO_TASK_8 will be skipped unless this has data';

----------------------------------------------------------------------
-- 2. Resume the DCM-managed failed-task alert
----------------------------------------------------------------------
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
