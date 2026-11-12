/*=============================================================================
  05_cleanup.sql: Build Data Pipelines with Snowflake DCM Projects

  IRREVERSIBLE. Purges the three projects of this guide and drops their project
  objects. Each team project is purged by the team role that owns it, then the
  Platform project by DCM_DEVELOPER.

  Objects shared by every guide in this series (DCM_DEMO, DCM_DEMO.PROJECTS,
  DCM_DEVELOPER and the inherited-grants opt-in) are left in place, commented.
=============================================================================*/

----------------------------------------------------------------------
-- 1. Purge the team projects
--
-- PURGE drops every entity a project manages, revokes its grants and removes
-- its attachments. The team projects go first, while their host databases
-- still exist: the Platform purge drops those databases.
----------------------------------------------------------------------
USE ROLE DCM_DEMO_2_FINANCE_DEV_ADMIN;
EXECUTE DCM PROJECT DCM_DEMO_2_FINANCE_DEV.PROJECTS.FINANCE_PIPELINE PURGE;

USE ROLE DCM_DEMO_2_MARKETING_DEV_ADMIN;
EXECUTE DCM PROJECT DCM_DEMO_2_MARKETING_DEV.PROJECTS.MARKETING_PIPELINE PURGE;

----------------------------------------------------------------------
-- 2. Drop the team project objects
----------------------------------------------------------------------
DROP DCM PROJECT IF EXISTS DCM_DEMO_2_MARKETING_DEV.PROJECTS.MARKETING_PIPELINE;

USE ROLE DCM_DEMO_2_FINANCE_DEV_ADMIN;
DROP DCM PROJECT IF EXISTS DCM_DEMO_2_FINANCE_DEV.PROJECTS.FINANCE_PIPELINE;

----------------------------------------------------------------------
-- 3. Purge the Platform project and drop its project object
--
-- This drops the raw and team databases, the warehouses, the team roles and
-- the READER database roles.
----------------------------------------------------------------------
USE ROLE DCM_DEVELOPER;
EXECUTE DCM PROJECT DCM_DEMO.PROJECTS.DCM_PLATFORM_DEV PURGE;
DROP DCM PROJECT IF EXISTS DCM_DEMO.PROJECTS.DCM_PLATFORM_DEV;

----------------------------------------------------------------------
-- 4. Objects created outside the projects
--
-- None. The sample files live in a stage the Platform project manages, so its
-- purge removed them.
----------------------------------------------------------------------

----------------------------------------------------------------------
-- 5. Series-shared objects, left commented
--
-- Every guide in this series uses these. Check what else lives in the shared
-- schema before you drop anything.
----------------------------------------------------------------------
SHOW DCM PROJECTS IN SCHEMA DCM_DEMO.PROJECTS;
-- USE ROLE ACCOUNTADMIN;
-- DROP SCHEMA IF EXISTS DCM_DEMO.PROJECTS;
-- DROP DATABASE IF EXISTS DCM_DEMO;
-- DROP ROLE IF EXISTS DCM_DEVELOPER;

----------------------------------------------------------------------
-- 6. Inherited grants, left commented
----------------------------------------------------------------------
-- ALTER ACCOUNT SET FEATURE_RBAC_INHERITED_GRANTS = 'DISABLED';

----------------------------------------------------------------------
-- 7. Verify
--
-- The first three come back empty. The last one still lists the project
-- objects of other guides in this series, if you deployed any; this guide's
-- DCM_PLATFORM_DEV is gone.
--
-- If a PURGE fails, suspend the projects' scheduled objects first so nothing
-- keeps consuming credits while you investigate:
--   USE ROLE DCM_DEVELOPER;
--   ALTER TASK DCM_DEMO_2_DEV.RAW.LOAD_NEW_DATA SUSPEND;
--   USE ROLE DCM_DEMO_2_FINANCE_DEV_ADMIN;
--   ALTER ALERT DCM_DEMO_2_FINANCE_DEV.SILVER.EXPECTATION_VIOLATIONS SUSPEND;
--   ALTER DYNAMIC TABLE DCM_DEMO_2_FINANCE_DEV.GOLD.DAILY_TRADING SUSPEND;
--   ALTER DYNAMIC TABLE DCM_DEMO_2_FINANCE_DEV.GOLD.CUSTOMER_TRADING SUSPEND;
--   ALTER DYNAMIC TABLE DCM_DEMO_2_FINANCE_DEV.SILVER.TRADES_ENRICHED SUSPEND;
----------------------------------------------------------------------
SHOW DATABASES  LIKE 'DCM_DEMO_2%';
SHOW WAREHOUSES LIKE 'DCM_DEMO_2%';
SHOW ROLES      LIKE 'DCM_DEMO_2%';
SHOW DCM PROJECTS IN SCHEMA DCM_DEMO.PROJECTS;
