/*=============================================================================
  01_pre_deploy.sql: DCM Projects for Tasks
  Run once, before the first plan, as ACCOUNTADMIN (the script switches roles
  where it needs to).

  DCM_DEVELOPER gets the superset of account-level privileges that every guide
  in this series needs, so one run of any guide's pre-deploy script prepares
  you for all of them. Each grant says what capability it buys. The script
  also creates the email notification integration the project's alert sends to.

  The DCM Project object itself is created by the first plan: select the
  DCM_DEV target and click Plan, and Snowsight asks you to confirm creating it.
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
-- statement succeeds. This guide does not use inherited grants itself; the
-- opt-in is here so the same role works for every guide in the series.
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

-- Set a masking policy on a tag (ALTER TAG ... SET MASKING POLICY).
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Create network policies and activate them (ATTACH POLICY); the application
-- role carries the rest of the network administration surface.
GRANT CREATE NETWORK POLICY ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT ATTACH POLICY         ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT APPLICATION ROLE SNOWFLAKE.NETWORK_SECURITY_ADMIN TO ROLE DCM_DEVELOPER;

-- Attach an authentication policy to the account or a user.
GRANT APPLY AUTHENTICATION POLICY ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- This guide only: read the task events in SNOWFLAKE.TELEMETRY.EVENTS_VIEW.
-- The TASKS_ERROR_RATE alert template reads them; without this application
-- role the alert's condition fails with "Schema 'SNOWFLAKE.TELEMETRY' does not
-- exist or not authorized".
GRANT APPLICATION ROLE SNOWFLAKE.EVENTS_VIEWER TO ROLE DCM_DEVELOPER;

-- This guide only: set LOG_EVENT_LEVEL on the project's database, which is what
-- makes the tasks log their run status for the alert. Without it the first
-- plan fails with "must have MODIFY LOG EVENT LEVEL granted on ACCOUNT".
GRANT MODIFY LOG EVENT LEVEL ON ACCOUNT TO ROLE DCM_DEVELOPER;

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
-- 5. Objects the project cannot own: the email integration
--
-- A DCM project cannot define notification integrations, so the email
-- integration the alert sends to is created here, as DCM_DEVELOPER. The role
-- then owns it, and ownership covers the USAGE the alert needs at send time.
-- With no ALLOWED_RECIPIENTS it can email any verified email address of a user
-- in this account.
----------------------------------------------------------------------
USE ROLE DCM_DEVELOPER;

CREATE NOTIFICATION INTEGRATION IF NOT EXISTS DCM_DEMO_4_EMAIL_INT
    TYPE = EMAIL
    ENABLED = TRUE
    COMMENT = 'Email integration for the DCM Projects for Tasks alert';

----------------------------------------------------------------------
-- 6. Project object (alternative path only)
--
-- You do not need this: selecting the DCM_DEV target and clicking Plan
-- prompts you to create the project object. Run it only if you prefer SQL.
----------------------------------------------------------------------
-- CREATE DCM PROJECT IF NOT EXISTS DCM_DEMO.PROJECTS.DCM_TASKS_PROJECT_DEV
--     COMMENT = 'for testing DCM Projects Quickstarts';

----------------------------------------------------------------------
-- 7. Values for the manifest
--
-- Paste ACCOUNT_IDENTIFIER into the account_identifier line of the DCM_DEV
-- target in DCM_Projects_Tasks/manifest.yml, and set alert_email to the
-- verified email address of your Snowflake user.
----------------------------------------------------------------------
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS ACCOUNT_IDENTIFIER,
       CURRENT_USER() AS USER_NAME;
