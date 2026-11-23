/*=============================================================================
  03_cleanup.sql: Get Started with Snowflake DCM Projects
  Run when you are finished, as DCM_DEVELOPER. IRREVERSIBLE: PURGE drops every
  object the project manages, with its data.

  Every guide in this series shares the DCM_DEMO.PROJECTS schema and the
  DCM_DEVELOPER role, so this script removes only what this guide owns and
  leaves the shared objects in place, commented out.
=============================================================================*/

USE ROLE DCM_DEVELOPER;

----------------------------------------------------------------------
-- 1. Purge the project
--
-- Drops every entity the project manages: the database, its schemas, tables,
-- dynamic table, semantic view, Streamlit app, secret, warehouse and roles,
-- with all their data. It also revokes the project's grants and removes its
-- attachments. PURGE renders no definitions, so it needs no environment values.
----------------------------------------------------------------------
EXECUTE DCM PROJECT DCM_DEMO.PROJECTS.DCM_PROJECT_DEV PURGE;

----------------------------------------------------------------------
-- 2. Drop the project object
--
-- PURGE leaves the project object and its deployment history behind.
----------------------------------------------------------------------
DROP DCM PROJECT IF EXISTS DCM_DEMO.PROJECTS.DCM_PROJECT_DEV;

----------------------------------------------------------------------
-- 3. Objects created outside the project
--
-- None: everything this guide deploys is defined in the project, and
-- 02_post_deploy.sql only inserts rows.
----------------------------------------------------------------------

----------------------------------------------------------------------
-- 4. Series-shared objects, left commented
--
-- The other guides in this series use these. Check what the schema still holds
-- before you drop anything; dropping DCM_DEMO destroys every project in it, and
-- dropping a role that owns objects orphans them.
----------------------------------------------------------------------
SHOW DCM PROJECTS IN SCHEMA DCM_DEMO.PROJECTS;
-- USE ROLE ACCOUNTADMIN;
-- DROP SCHEMA IF EXISTS DCM_DEMO.PROJECTS;
-- DROP DATABASE IF EXISTS DCM_DEMO;
-- DROP ROLE IF EXISTS DCM_DEVELOPER;

----------------------------------------------------------------------
-- 5. Inherited grants, left commented
--
-- Account-wide and needed by the other guides. To turn the opt-in back off:
----------------------------------------------------------------------
-- ALTER ACCOUNT SET FEATURE_RBAC_INHERITED_GRANTS = 'DISABLED';

----------------------------------------------------------------------
-- 6. Verify
--
-- IF PURGE FAILED: the project's objects are still live and may still consume
-- credits. Suspend anything scheduled before you investigate:
--   ALTER DYNAMIC TABLE DCM_DEMO_1_DEV.ANALYTICS.ENRICHED_ORDER_DETAILS SUSPEND;
--   ALTER WAREHOUSE IF EXISTS DCM_DEMO_1_WH_DEV SUSPEND;
----------------------------------------------------------------------
SHOW DATABASES LIKE 'DCM_DEMO_1_DEV';
SHOW WAREHOUSES LIKE 'DCM_DEMO_1_WH_DEV';
-- The first two come back empty. The third no longer lists DCM_PROJECT_DEV but
-- still lists any other guide's project in the shared schema.
SHOW DCM PROJECTS IN SCHEMA DCM_DEMO.PROJECTS;
