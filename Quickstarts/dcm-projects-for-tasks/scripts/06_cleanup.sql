/*=============================================================================
  06_cleanup.sql: DCM Projects for Tasks
  Run when you are done, as DCM_DEVELOPER. IRREVERSIBLE: PURGE drops every
  object the project manages, including the tables and their data.
=============================================================================*/

USE ROLE DCM_DEVELOPER;

----------------------------------------------------------------------
-- 1. Purge the project
--
-- Drops every entity the project manages (the warehouse, the database with
-- its tables, the task graph and its member tasks, and the alert), revokes
-- the grants it made and removes its attachments.
----------------------------------------------------------------------
EXECUTE DCM PROJECT DCM_DEMO.PROJECTS.DCM_TASKS_PROJECT_DEV PURGE;

----------------------------------------------------------------------
-- 2. Drop the project object
--
-- PURGE leaves the project object itself behind.
----------------------------------------------------------------------
DROP DCM PROJECT IF EXISTS DCM_DEMO.PROJECTS.DCM_TASKS_PROJECT_DEV;

----------------------------------------------------------------------
-- 3. Objects created outside the project
--
-- 02_guide_setup.sql created the email integration, so PURGE does not touch
-- it. Only this guide uses it. DCM_DEVELOPER owns it and can drop it.
----------------------------------------------------------------------
DROP INTEGRATION IF EXISTS DCM_DEMO_4_EMAIL_INT;

----------------------------------------------------------------------
-- 4. Series-shared objects, left commented
--
-- Every guide in the DCM series uses these. Check what else lives in the
-- shared schema before you drop anything:
----------------------------------------------------------------------
SHOW DCM PROJECTS IN SCHEMA DCM_DEMO.PROJECTS;

-- USE ROLE ACCOUNTADMIN;
-- DROP SCHEMA IF EXISTS DCM_DEMO.PROJECTS;
-- DROP DATABASE IF EXISTS DCM_DEMO;
-- DROP ROLE IF EXISTS DCM_DEVELOPER;

----------------------------------------------------------------------
-- 5. Inherited grants, left commented
----------------------------------------------------------------------
-- ALTER ACCOUNT SET FEATURE_RBAC_INHERITED_GRANTS = 'DISABLED';

----------------------------------------------------------------------
-- 6. Verify
--
-- The first three checks come back empty. The last one still lists the
-- projects of any other guides in this series that you have not cleaned up.
-- If PURGE failed, stop everything that runs on a schedule before you
-- investigate, so nothing keeps consuming credits:
--   ALTER TASK GRAPH DCM_DEMO_4_DEV.PIPELINE.WEATHER_PIPELINE SUSPEND;
--   ALTER ALERT DCM_DEMO_4_DEV.PIPELINE.TASK_ERROR_RATE SUSPEND;
----------------------------------------------------------------------
SHOW DATABASES LIKE 'DCM_DEMO_4_DEV';
SHOW WAREHOUSES LIKE 'DCM_DEMO_4_WH_DEV';
SHOW INTEGRATIONS LIKE 'DCM_DEMO_4_EMAIL_INT';
SHOW DCM PROJECTS IN SCHEMA DCM_DEMO.PROJECTS;
