-- ### Alert on failing tasks
--
-- TASKS_ERROR_RATE reads the event table and notifies when the share of failed
-- task runs in the SEGMENTS schema passes the threshold. The task graph's
-- ON_FAILURE branch records why a run stopped; this alert tells someone that
-- runs are failing.
--
-- IN_APP sends the notification to the Snowsight user who deployed the alert.
-- STARTED, last in the statement, makes DCM deploy the alert running.

DEFINE ALERT DCM_DEMO_2_MARKETING{{env_suffix}}.SEGMENTS.TASK_FAILURES
    FROM TEMPLATE TASKS_ERROR_RATE
    WAREHOUSE = DCM_DEMO_2_MARKETING_WH{{env_suffix}}
    SCHEDULE = '30 MINUTE'
    COMMENT = 'Notifies when too many task runs in the SEGMENTS schema fail'
    TEMPLATE_PARAMS = '{"template_variables": {
        "ERROR_RATE_THRESHOLD": 0.15,
        "SCOPE_ACTIVE": "SCHEMA",
        "SCOPE_DATABASE": "DCM_DEMO_2_MARKETING{{env_suffix}}",
        "SCOPE_SCHEMA": "SEGMENTS",
        "NOTIFICATION_MODE": "IN_APP"}}'
    STARTED;
