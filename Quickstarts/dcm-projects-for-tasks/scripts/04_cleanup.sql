/*=============================================================================
  04_cleanup.sql: run when you are done

  Drops only what this guide owns. The DCM_DEMO database, its PROJECTS schema
  and the DCM_DEVELOPER role are shared by every guide in the DCM series, so
  they are left in place, commented out.
=============================================================================*/

USE ROLE DCM_DEVELOPER;

----------------------------------------------------------------------
-- 1. Check what else lives in the shared schema
----------------------------------------------------------------------
SHOW DCM PROJECTS IN SCHEMA DCM_DEMO.PROJECTS;

----------------------------------------------------------------------
-- 2. Purge everything this project manages, then drop the project
--
-- PURGE drops the database, its tables and their data, the task graph and its
-- member tasks, and the warehouse. It cannot be undone, and it leaves the
-- project object behind, which is why the DROP follows.
--
-- If PURGE fails, stop the schedule before you investigate:
--   ALTER TASK GRAPH DCM_DEMO_4_DEV.PIPELINE.WEATHER_PIPELINE SUSPEND;
----------------------------------------------------------------------
EXECUTE DCM PROJECT DCM_DEMO.PROJECTS.DCM_TASKS_PROJECT_DEV PURGE;

DROP DCM PROJECT IF EXISTS DCM_DEMO.PROJECTS.DCM_TASKS_PROJECT_DEV;

----------------------------------------------------------------------
-- 3. Shared objects, left alone on purpose
----------------------------------------------------------------------
-- DROP SCHEMA IF EXISTS DCM_DEMO.PROJECTS;
-- DROP DATABASE IF EXISTS DCM_DEMO;
-- USE ROLE ACCOUNTADMIN;
-- DROP ROLE IF EXISTS DCM_DEVELOPER;

----------------------------------------------------------------------
-- 4. Verify
----------------------------------------------------------------------
SHOW DATABASES LIKE 'DCM_DEMO_4_DEV';
SHOW WAREHOUSES LIKE 'DCM_DEMO_4_WH_DEV';
