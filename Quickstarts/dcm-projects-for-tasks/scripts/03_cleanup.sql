/*=============================================================================
  03_cleanup.sql — Tear down all objects created by this quickstart
=============================================================================*/

USE ROLE dcm_developer;

-- Suspend and drop the dev database (cascades to tables, tasks, streams, alerts, DMFs)
ALTER TASK IF EXISTS dcm_demo_4_dev.pipeline.demo_task_1 SUSPEND;
DROP DATABASE IF EXISTS dcm_demo_4_dev;
DROP WAREHOUSE IF EXISTS dcm_demo_4_wh_dev;

DROP ROLE IF EXISTS dcm_demo_4_dev_read;

USE ROLE ACCOUNTADMIN;
DROP INTEGRATION IF EXISTS dcm_demo_email_notifications;

DROP DCM PROJECT IF EXISTS dcm_demo.projects.dcm_project_dev;
DROP SCHEMA IF EXISTS dcm_demo.projects;
DROP DATABASE IF EXISTS dcm_demo;

DROP ROLE IF EXISTS dcm_developer;
DROP WAREHOUSE IF EXISTS dcm_wh;
