/*=============================================================================
  alerts.sql: one alert that watches every task in the project's database

  TASKS_ERROR_RATE is an alert template: Snowflake renders the condition and
  the notification action from the parameters below. It reads the task run
  events that LOG_EVENT_LEVEL (infrastructure.sql) turns on, and emails
  alert_email (manifest.yml) when more than 10% of the task status events
  in the window are failures. 0.1 is sized from a measured run of this demo;
  a real pipeline uses a higher threshold (the template default is 0.15).

  An alert from a template cannot be declared STARTED, so DCM deploys it
  suspended. scripts/02_post_deploy.sql resumes it.
=============================================================================*/

DEFINE ALERT DCM_DEMO_4{{env_suffix}}.PIPELINE.TASK_ERROR_RATE
    FROM TEMPLATE TASKS_ERROR_RATE
    WAREHOUSE = DCM_DEMO_4_WH{{env_suffix}}
    SCHEDULE = '60 MINUTE'
    COMMENT = 'Emails when more than 10% of task runs in the database failed'
    TEMPLATE_PARAMS = '{"template_variables":{"ERROR_RATE_THRESHOLD":0.1,"SCOPE_ACTIVE":"DATABASE","SCOPE_DATABASE":"DCM_DEMO_4{{env_suffix}}","NOTIFICATION_MODE":"EMAIL","EMAIL_NOTIFICATION_INTEGRATION":"DCM_DEMO_4_EMAIL_INT"},"notification_config":{"notification_integration":"DCM_DEMO_4_EMAIL_INT","email_config":{"toAddress":["{{alert_email}}"]}}}';
