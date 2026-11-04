/*=============================================================================
  01_pre_deploy.sql: Build Data Pipelines with Snowflake DCM Projects

  Run once, BEFORE the first plan, as ACCOUNTADMIN (the script switches roles
  where it needs to).

  Creates the DCM_DEVELOPER role shared by every guide in this series, turns on
  inherited grants, grants the account-level privileges a project cannot grant
  itself, and creates the container schema for the project objects.

  The DCM Project objects themselves are created by the first plan: select the
  target in the DCM control panel and click Plan, and Snowsight offers to create
  the object named in the manifest. Section 6 shows the SQL alternative.
=============================================================================*/

----------------------------------------------------------------------
-- 1. Role
----------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS DCM_DEVELOPER
    COMMENT = 'Owns and deploys the DCM Projects quickstart projects';
SET MY_USER = CURRENT_USER();
GRANT ROLE DCM_DEVELOPER TO USER IDENTIFIER($MY_USER);
GRANT ROLE DCM_DEVELOPER TO ROLE SYSADMIN;

----------------------------------------------------------------------
-- 2. Inherited grants opt-in
--
-- One-time, account-wide opt-in. Required before any GRANT INHERITED statement
-- succeeds; the Platform project uses inherited grants on the RAW schema.
----------------------------------------------------------------------
ALTER ACCOUNT SET FEATURE_RBAC_INHERITED_GRANTS = 'ENABLED';

----------------------------------------------------------------------
-- 3. Account-level privileges
--
-- A project cannot grant account-level privileges to its own owner, so they all
-- happen here. DCM_DEVELOPER gets the superset every guide in this series uses,
-- so one run of any guide's setup prepares you for all of them.
----------------------------------------------------------------------

-- Create account-level objects: the Platform project creates databases,
-- warehouses and team roles. MANAGE GRANTS lets the owner grant on objects it
-- does not own, which is what makes GRANT statements inside a project work.
GRANT CREATE DATABASE  ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT CREATE ROLE      ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT MANAGE GRANTS    ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Run tasks. EXECUTE TASK runs tasks the role owns, which is how this guide
-- runs the load task by hand. EXECUTE MANAGED TASK is needed only for serverless
-- tasks, which other guides in the series use.
GRANT EXECUTE TASK         ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Run alerts, same split as tasks. Used by other guides in the series.
GRANT EXECUTE ALERT         ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT EXECUTE MANAGED ALERT ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Create integrations outside a project, for example the notification
-- integration an alert sends through. Used by other guides in the series.
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- See task and pipe history across the account.
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Attach and evaluate data quality expectations. The VIEWER application role
-- reads their results. WITH GRANT OPTION is what lets the Platform project pass
-- EXECUTE DATA METRIC FUNCTION on to the Finance team role; without it the
-- first plan fails with "Grant not executed: Insufficient privileges".
GRANT EXECUTE DATA METRIC FUNCTION ON ACCOUNT TO ROLE DCM_DEVELOPER WITH GRANT OPTION;
GRANT APPLICATION ROLE SNOWFLAKE.DATA_QUALITY_MONITORING_VIEWER TO ROLE DCM_DEVELOPER;

-- Configure data quality notifications on a database. Used by other guides.
GRANT MANAGE DATA QUALITY ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Set a masking policy on a tag after deploy (the governance guide's
-- ALTER TAG ... SET MASKING POLICY).
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Create and activate network policies. Used by other guides in the series.
GRANT CREATE NETWORK POLICY ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT ATTACH POLICY         ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT APPLICATION ROLE SNOWFLAKE.NETWORK_SECURITY_ADMIN TO ROLE DCM_DEVELOPER;

-- Attach an authentication policy to the account or a user. Used by other
-- guides in the series.
GRANT APPLY AUTHENTICATION POLICY ON ACCOUNT TO ROLE DCM_DEVELOPER;

----------------------------------------------------------------------
-- 4. Series container
----------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS DCM_DEMO
    COMMENT = 'Holds the DCM Project objects of the DCM Projects quickstarts';
CREATE SCHEMA IF NOT EXISTS DCM_DEMO.PROJECTS
    COMMENT = 'DCM Project objects of the DCM Projects quickstarts';
GRANT USAGE ON DATABASE DCM_DEMO TO ROLE DCM_DEVELOPER;
GRANT USAGE ON SCHEMA DCM_DEMO.PROJECTS TO ROLE DCM_DEVELOPER;
GRANT CREATE DCM PROJECT ON SCHEMA DCM_DEMO.PROJECTS TO ROLE DCM_DEVELOPER;

----------------------------------------------------------------------
-- 5. Objects the project cannot own: none in this guide.
----------------------------------------------------------------------

----------------------------------------------------------------------
-- 6. Project objects (alternative path only)
--
-- The first plan creates each project object for you. If the prompt does not
-- appear, or you prefer SQL, run the Platform line now as DCM_DEVELOPER. The
-- Pipeline project lives in a database the Platform project creates, so its
-- line only works after the Platform deploy, as the Finance team role.
----------------------------------------------------------------------
-- USE ROLE DCM_DEVELOPER;
-- CREATE DCM PROJECT IF NOT EXISTS DCM_DEMO.PROJECTS.DCM_PLATFORM_DEV
--     COMMENT = 'for testing DCM Projects Quickstarts';
-- USE ROLE DCM_DEMO_2_FINANCE_DEV_ADMIN;
-- CREATE DCM PROJECT IF NOT EXISTS DCM_DEMO_2_FINANCE_DEV.PROJECTS.FINANCE_PIPELINE
--     COMMENT = 'for testing DCM Projects Quickstarts';

----------------------------------------------------------------------
-- 7. Values for the manifest
--
-- Paste ACCOUNT_IDENTIFIER into the DCM_DEV account_identifier line of both
-- DCM_Platform_Demo/manifest.yml and DCM_Pipeline_Demo/manifest.yml.
----------------------------------------------------------------------
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS ACCOUNT_IDENTIFIER,
       CURRENT_USER() AS USER_NAME;
