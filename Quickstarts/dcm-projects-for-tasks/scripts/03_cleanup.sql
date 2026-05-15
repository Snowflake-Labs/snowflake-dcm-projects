/*=============================================================================
  03_cleanup.sql — Tear down all objects created by this quickstart
=============================================================================*/

-- PURGE drops every object the project created: databases, warehouses, roles,
-- grants, tables, tasks, procedures, alerts — everything managed by the project.
USE ROLE dcm_developer;
EXECUTE DCM PROJECT dcm_demo.projects.dcm_tasks_project_dev PURGE;

-- Notification integration is outside project scope, drop separately
USE ROLE ACCOUNTADMIN;
DROP INTEGRATION IF EXISTS dcm_demo_email_notifications;

DROP DCM PROJECT IF EXISTS dcm_demo.projects.dcm_tasks_project_dev;
DROP SCHEMA IF EXISTS dcm_demo.projects;
DROP DATABASE IF EXISTS dcm_demo;

DROP ROLE IF EXISTS dcm_developer;
