-- Serverless alert that monitors the task graph and emails on failures.
--
-- Defined with DCM so the alert is managed alongside the rest of the project.
-- The alert is created in a suspended state — resume it in the post-deploy
-- script with `ALTER ALERT ... RESUME`.
-- Recipient email comes from the {{notification_recipient}} manifest value,
-- the same one the finalizer uses, so you only configure it once.

DEFINE ALERT DCM_DEMO_4{{env_suffix}}.PIPELINE.FAILED_TASK_ALERT
    SCHEDULE = '60 MINUTE'
    IF (EXISTS (
        SELECT NAME, SCHEMA_NAME
        FROM TABLE(DCM_DEMO_4{{env_suffix}}.INFORMATION_SCHEMA.TASK_HISTORY(
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
                    ARRAY_CONSTRUCT('{{notification_recipient}}'),
                    NULL, NULL));
        END;
