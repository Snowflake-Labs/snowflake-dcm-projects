/*=============================================================================
  01_pre_deploy.sql: DCM Projects for Dynamic Tables
  Run once, before the first plan, as ACCOUNTADMIN (the script switches roles
  where it needs to).

  DCM_DEVELOPER gets the superset of account-level privileges that every guide
  in this series needs, so one run of any guide's pre-deploy script prepares
  you for all of them. Each grant says what capability it buys.
=============================================================================*/

----------------------------------------------------------------------
-- 1. Role
----------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS DCM_DEVELOPER
    COMMENT = 'Owner role for the DCM Projects quickstarts';
SET MY_USER = CURRENT_USER();
GRANT ROLE DCM_DEVELOPER TO USER IDENTIFIER($MY_USER);
GRANT ROLE DCM_DEVELOPER TO ROLE SYSADMIN;

----------------------------------------------------------------------
-- 2. Inherited grants opt-in
--
-- A one-time, account-wide opt-in, required before any GRANT INHERITED
-- statement succeeds. access.sql uses inherited grants for the READ role.
----------------------------------------------------------------------
ALTER ACCOUNT SET FEATURE_RBAC_INHERITED_GRANTS = 'ENABLED';

----------------------------------------------------------------------
-- 3. Account-level privileges
--
-- A project cannot grant account-level privileges to its own owner, so they
-- all happen here. Some are checked at PLAN time: a definition that needs a
-- missing account privilege fails the first plan, not the deploy.
----------------------------------------------------------------------

-- Create the account-level objects a project manages (databases, warehouses,
-- roles), and grant on objects the owner does not own.
GRANT CREATE DATABASE  ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT CREATE ROLE      ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT MANAGE GRANTS    ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Run tasks the owner owns; serverless tasks additionally need EXECUTE MANAGED TASK.
GRANT EXECUTE TASK         ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Run alerts the owner owns; serverless alerts additionally need EXECUTE MANAGED ALERT.
GRANT EXECUTE ALERT         ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT EXECUTE MANAGED ALERT ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Create integrations (notification, API, storage, external access).
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- See task and pipe history across the account.
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Attach data quality expectations (EXECUTE DATA METRIC FUNCTION), read their
-- results (VIEWER application role), and configure data quality notifications
-- on a database (MANAGE DATA QUALITY).
GRANT EXECUTE DATA METRIC FUNCTION ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT APPLICATION ROLE SNOWFLAKE.DATA_QUALITY_MONITORING_VIEWER TO ROLE DCM_DEVELOPER;
GRANT MANAGE DATA QUALITY ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Set a masking policy on a tag after deploy (the governance guide's
-- ALTER TAG ... SET MASKING POLICY).
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Create network policies and activate them (ATTACH POLICY); the application
-- role carries the rest of the network administration surface.
GRANT CREATE NETWORK POLICY ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT ATTACH POLICY         ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT APPLICATION ROLE SNOWFLAKE.NETWORK_SECURITY_ADMIN TO ROLE DCM_DEVELOPER;

-- Attach an authentication policy to the account or a user.
GRANT APPLY AUTHENTICATION POLICY ON ACCOUNT TO ROLE DCM_DEVELOPER;

----------------------------------------------------------------------
-- 4. Series container
--
-- Holds the DCM Project object of every guide in this series.
----------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS DCM_DEMO
    COMMENT = 'Container for the DCM Projects quickstarts project objects';
CREATE SCHEMA IF NOT EXISTS DCM_DEMO.PROJECTS
    COMMENT = 'DCM Project objects for the DCM Projects quickstarts';
GRANT USAGE ON DATABASE DCM_DEMO TO ROLE DCM_DEVELOPER;
GRANT USAGE ON SCHEMA DCM_DEMO.PROJECTS TO ROLE DCM_DEVELOPER;
GRANT CREATE DCM PROJECT ON SCHEMA DCM_DEMO.PROJECTS TO ROLE DCM_DEVELOPER;

----------------------------------------------------------------------
-- 5. Objects the project cannot own: none in this guide.
----------------------------------------------------------------------

----------------------------------------------------------------------
-- 6. Project object (alternative path only)
--
-- You do not need this: selecting the DCM_DEV target and clicking Plan
-- prompts you to create the project object. Run it only if you prefer SQL.
----------------------------------------------------------------------
-- USE ROLE DCM_DEVELOPER;
-- CREATE DCM PROJECT IF NOT EXISTS DCM_DEMO.PROJECTS.DCM_DT_PROJECT_DEV
--     COMMENT = 'for testing DCM Projects Quickstarts';

----------------------------------------------------------------------
-- 7. Values for the manifest
--
-- Paste ACCOUNT_IDENTIFIER into the account_identifier line of the DCM_DEV
-- target in manifest.yml.
----------------------------------------------------------------------
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS ACCOUNT_IDENTIFIER,
       CURRENT_USER() AS USER_NAME;
