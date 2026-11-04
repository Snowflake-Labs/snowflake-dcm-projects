/*=============================================================================
  04_cleanup.sql: DCM Projects for Dynamic Tables
  Irreversible. Run as DCM_DEVELOPER when you are finished with this guide.
=============================================================================*/

USE ROLE DCM_DEVELOPER;

----------------------------------------------------------------------
-- 1. Purge the project
--
-- Drops every entity the project manages (the database and everything in it,
-- the warehouse and the READ role), revokes its grants and removes its
-- expectation attachments, with all of their data.
----------------------------------------------------------------------
EXECUTE DCM PROJECT DCM_DEMO.PROJECTS.DCM_DT_PROJECT_DEV PURGE;

----------------------------------------------------------------------
-- 2. Drop the project object
----------------------------------------------------------------------
DROP DCM PROJECT IF EXISTS DCM_DEMO.PROJECTS.DCM_DT_PROJECT_DEV;

----------------------------------------------------------------------
-- 3. Objects created outside the project
--
-- None: this guide creates everything through the project.
----------------------------------------------------------------------

----------------------------------------------------------------------
-- 4. Series-shared objects, left commented
--
-- Every guide in this series uses these. Check what the schema still holds
-- before dropping anything.
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
--
-- If PURGE fails, suspend the project's scheduled objects first so nothing
-- keeps consuming credits while you investigate:
--   ALTER TASK DCM_DEMO_3_DEV.RAW.TSK_INGEST_DAILY_ORDERS SUSPEND;
--   ALTER DYNAMIC TABLE DCM_DEMO_3_DEV.ANALYTICS.ENRICHED_ORDER_DETAILS SUSPEND;
----------------------------------------------------------------------
SHOW DATABASES LIKE 'DCM_DEMO_3_DEV';
SHOW WAREHOUSES LIKE 'DCM_DEMO_3_WH_DEV';
SHOW ROLES LIKE 'DCM_DEMO_3_DEV_READ';
SHOW DCM PROJECTS IN SCHEMA DCM_DEMO.PROJECTS;
