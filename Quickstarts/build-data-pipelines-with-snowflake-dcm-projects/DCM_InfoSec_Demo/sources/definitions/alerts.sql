-- ### Alerts on the streaming pipes
--
-- Two template alerts read the event table for the SECURITY schema. They see
-- pipe events because the Platform project sets LOG_EVENT_LEVEL = WARN on
-- every team database.
--
-- STREAMING_ROW_ERRORS notifies when the share of streamed rows that could not
-- be ingested passes 10 percent. Those rows are in the target tables' error
-- tables (ERROR_LOGGING = TRUE in streaming.sql). MIN_ROWS_PARSED keeps a
-- handful of bad rows in a quiet period from firing it.
--
-- PIPE_ERRORS notifies when a pipe stops or enters an error state. A pipe
-- stops, for example, if its owner loses SELECT on a raw table it joins, so
-- this alert is also the one that notices a change in the Platform project.
--
-- IN_APP sends the notification to the Snowsight user who deployed the alert.
-- STARTED, last in each statement, makes DCM deploy the alert running.

DEFINE ALERT DCM_DEMO_2_INFOSEC{{env_suffix}}.SECURITY.STREAMING_ROW_ERRORS
    FROM TEMPLATE SNOWPIPE_STREAMING_ROW_ERROR_RATE
    WAREHOUSE = DCM_DEMO_2_INFOSEC_WH{{env_suffix}}
    SCHEDULE = '5 MINUTE'
    COMMENT = 'Notifies when more than 10 percent of streamed rows fail to ingest'
    TEMPLATE_PARAMS = '{"template_variables": {
        "ERROR_RATE_THRESHOLD": 0.1,
        "MIN_ROWS_PARSED": 100,
        "SCOPE_ACTIVE": "SCHEMA",
        "SCOPE_DATABASE": "DCM_DEMO_2_INFOSEC{{env_suffix}}",
        "SCOPE_SCHEMA": "SECURITY",
        "NOTIFICATION_MODE": "IN_APP"}}'
    STARTED;

DEFINE ALERT DCM_DEMO_2_INFOSEC{{env_suffix}}.SECURITY.PIPE_ERRORS
    FROM TEMPLATE SNOWPIPE_PIPE_ERROR
    WAREHOUSE = DCM_DEMO_2_INFOSEC_WH{{env_suffix}}
    SCHEDULE = '5 MINUTE'
    COMMENT = 'Notifies when a streaming pipe in the SECURITY schema stops or errors'
    TEMPLATE_PARAMS = '{"template_variables": {
        "SCOPE_ACTIVE": "SCHEMA",
        "SCOPE_DATABASE": "DCM_DEMO_2_INFOSEC{{env_suffix}}",
        "SCOPE_SCHEMA": "SECURITY",
        "NOTIFICATION_MODE": "IN_APP"}}'
    STARTED;
