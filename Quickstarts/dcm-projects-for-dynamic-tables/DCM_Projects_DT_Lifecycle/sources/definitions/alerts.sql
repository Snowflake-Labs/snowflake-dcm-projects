/*=============================================================================
  alerts.sql: one alert that watches every dynamic table in the project's
  database

  DT_REFRESH_FAILURE is an alert template: Snowflake renders the condition and
  the notification action from the parameters below. It reads the refresh
  failure events that LOG_EVENT_LEVEL (raw.sql) turns on, and posts an in-app
  notification in Snowsight when a dynamic table refresh fails. IN_APP needs
  no notification integration.

  An alert from a template cannot be declared STARTED, so DCM deploys it
  suspended. scripts/02_post_deploy.sql resumes it.
=============================================================================*/

DEFINE ALERT DCM_DEMO_3{{env_suffix}}.ANALYTICS.DT_REFRESH_FAILURE
    FROM TEMPLATE DT_REFRESH_FAILURE
    WAREHOUSE = DCM_DEMO_3_WH{{env_suffix}}
    SCHEDULE = '60 MINUTE'
    COMMENT = 'Notifies in Snowsight when a dynamic table refresh in the database fails'
    TEMPLATE_PARAMS = '{"template_variables":{"SCOPE_ACTIVE":"DATABASE","SCOPE_DATABASE":"DCM_DEMO_3{{env_suffix}}","NOTIFICATION_MODE":"IN_APP"}}';
