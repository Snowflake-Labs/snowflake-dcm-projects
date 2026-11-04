/*=============================================================================
  05_cleanup.sql: Govern Data with Snowflake DCM Projects
  Run when you are finished. IRREVERSIBLE. Run as DCM_DEVELOPER.

  Drops only what this guide owns. The series-shared container, role and
  account parameter are left in place, commented out, because the other
  guides in this series use them.
=============================================================================*/

USE ROLE DCM_DEVELOPER;

----------------------------------------------------------------------
-- 1. Remove the associations made outside DCM
--
-- 03_post_deploy.sql made these with ALTER statements, so PURGE does not know
-- about them. Remove them first: a network policy can't be dropped while it
-- is set on a user, and PURGE fails on a masking policy that is still set on
-- a tag.
----------------------------------------------------------------------
ALTER USER IF EXISTS DCM_DEMO_5_DEV_SVC UNSET NETWORK_POLICY;
ALTER USER IF EXISTS DCM_DEMO_5_DEV_SVC UNSET AUTHENTICATION POLICY;
ALTER TAG IF EXISTS DCM_DEMO_5_DEV.GOV.PII UNSET MASKING POLICY DCM_DEMO_5_DEV.GOV.EMAIL_MASK;

----------------------------------------------------------------------
-- 2. Purge the project
--
-- Drops every entity the project manages (database, schemas, table, tags,
-- policies, network rule and policy, warehouse, analyst role and all data),
-- revokes its grants and removes its attachments. PURGE is not transactional:
-- if it fails, some objects may already be gone. The known cause here is a
-- masking policy still set on a tag; re-run section 1, then re-run PURGE.
----------------------------------------------------------------------
EXECUTE DCM PROJECT DCM_DEMO.PROJECTS.DCM_GOV_PROJECT_DEV PURGE;

----------------------------------------------------------------------
-- 3. Drop the project object
----------------------------------------------------------------------
DROP DCM PROJECT IF EXISTS DCM_DEMO.PROJECTS.DCM_GOV_PROJECT_DEV;

----------------------------------------------------------------------
-- 4. Objects created outside the project
----------------------------------------------------------------------
DROP USER IF EXISTS DCM_DEMO_5_DEV_SVC;

----------------------------------------------------------------------
-- 5. Series-shared objects, left commented
--
-- DCM_DEMO.PROJECTS holds the project objects of every DCM guide you have
-- run, and DCM_DEVELOPER is the role every guide uses. Check what is still
-- there before uncommenting anything.
----------------------------------------------------------------------
SHOW DCM PROJECTS IN SCHEMA DCM_DEMO.PROJECTS;
-- USE ROLE ACCOUNTADMIN;
-- DROP SCHEMA IF EXISTS DCM_DEMO.PROJECTS;
-- DROP DATABASE IF EXISTS DCM_DEMO;
-- DROP ROLE IF EXISTS DCM_DEVELOPER;

----------------------------------------------------------------------
-- 6. Inherited grants, left commented
--
-- Account-wide, and the other guides in this series need it.
----------------------------------------------------------------------
-- ALTER ACCOUNT SET FEATURE_RBAC_INHERITED_GRANTS = 'DISABLED';

----------------------------------------------------------------------
-- 7. Verify
--
-- The first three come back empty. The project list no longer contains
-- DCM_GOV_PROJECT_DEV, but still lists the projects of any other guide in
-- this series you ran. If PURGE fails, there is nothing scheduled to suspend
-- first: this guide defines no tasks, alerts or dynamic tables.
----------------------------------------------------------------------
SHOW DATABASES LIKE 'DCM_DEMO_5_DEV';
SHOW WAREHOUSES LIKE 'DCM_DEMO_5_WH_DEV';
SHOW USERS LIKE 'DCM_DEMO_5_DEV_SVC';
SHOW DCM PROJECTS IN SCHEMA DCM_DEMO.PROJECTS;
