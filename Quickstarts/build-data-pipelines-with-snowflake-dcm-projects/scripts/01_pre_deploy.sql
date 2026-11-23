/*=============================================================================
  01_pre_deploy.sql: DCM Projects quickstart series, shared setup

  The same script ships with every guide in the series. Run it once, BEFORE the
  first plan, as ACCOUNTADMIN; it is idempotent, so running it again from
  another guide changes nothing.

  Creates the DCM_DEVELOPER role, turns on inherited grants, grants the role the
  account-level privileges a project cannot grant its own owner (the union of
  what every guide in the series needs), and creates the DCM_DEMO.PROJECTS
  schema that holds the project objects.

  Objects a guide needs that a project cannot own (a notification integration,
  a demo user) are created by that guide's 02_guide_setup.sql, not here.

  The script ends by creating this guide's DCM Project object (section 5), the
  one line that differs between guides. Refresh the browser afterwards so the
  DCM control panel lists it.
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
-- A one-time, account-wide opt-in, required before any GRANT INHERITED
-- statement succeeds. Every guide's access grants except Tasks use them.
----------------------------------------------------------------------
ALTER ACCOUNT SET FEATURE_RBAC_INHERITED_GRANTS = 'ENABLED';

----------------------------------------------------------------------
-- 3. Account-level privileges
--
-- A project cannot grant account-level privileges to its own owner, so they
-- all happen here. Some are checked at PLAN time: a definition that needs a
-- missing one fails the first plan, not the deploy. Each comment names the
-- guide or feature that uses the grant.
----------------------------------------------------------------------

-- All guides: create the databases, warehouses and roles a project defines.
GRANT CREATE DATABASE  ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT CREATE ROLE      ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- All guides: grant on objects the owner does not own (GRANT inside a project).
GRANT MANAGE GRANTS ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Tasks, Dynamic Tables, Pipelines: run tasks; MANAGED for serverless tasks.
GRANT EXECUTE TASK         ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Tasks, Dynamic Tables: run the template alerts; MANAGED for serverless ones.
GRANT EXECUTE ALERT         ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT EXECUTE MANAGED ALERT ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Tasks: create the email notification integration in 02_guide_setup.sql.
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Tasks, Pipelines: see task and pipe history across the account.
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Data quality expectations (all but Tasks): attach DMFs. WITH GRANT OPTION is
-- what lets the Pipelines Platform project pass it to the Finance team role;
-- without it that plan fails with "Grant not executed: Insufficient privileges".
GRANT EXECUTE DATA METRIC FUNCTION ON ACCOUNT TO ROLE DCM_DEVELOPER WITH GRANT OPTION;

-- Data quality expectations: read results; Pipelines also grants it to Finance.
GRANT APPLICATION ROLE SNOWFLAKE.DATA_QUALITY_MONITORING_VIEWER TO ROLE DCM_DEVELOPER;

-- Governance: data quality notifications (DATA_QUALITY_MONITORING_SETTINGS).
GRANT MANAGE DATA QUALITY ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Governance: tag-based masking (ALTER TAG ... SET MASKING POLICY).
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Governance: create network policies and activate them (ATTACH POLICY).
GRANT CREATE NETWORK POLICY ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT ATTACH POLICY         ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT APPLICATION ROLE SNOWFLAKE.NETWORK_SECURITY_ADMIN TO ROLE DCM_DEVELOPER;

-- Governance: set an authentication policy on the demo service user.
GRANT APPLY AUTHENTICATION POLICY ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Tasks, Dynamic Tables: the alerts read SNOWFLAKE.TELEMETRY.EVENTS_VIEW;
-- without it the condition fails "Schema 'SNOWFLAKE.TELEMETRY' does not exist".
GRANT APPLICATION ROLE SNOWFLAKE.EVENTS_VIEWER TO ROLE DCM_DEVELOPER;

-- Tasks, Dynamic Tables: set LOG_EVENT_LEVEL on the project database; without
-- it the first plan fails "must have MODIFY LOG EVENT LEVEL granted on ACCOUNT".
GRANT MODIFY LOG EVENT LEVEL ON ACCOUNT TO ROLE DCM_DEVELOPER;

----------------------------------------------------------------------
-- 4. Series container
--
-- DCM_DEMO.PROJECTS holds the project object of every guide in the series.
-- A project cannot define the database or schema it lives in.
----------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS DCM_DEMO
    COMMENT = 'Holds the DCM Project objects of the DCM Projects quickstarts';
CREATE SCHEMA IF NOT EXISTS DCM_DEMO.PROJECTS
    COMMENT = 'DCM Project objects of the DCM Projects quickstarts';
GRANT USAGE ON DATABASE DCM_DEMO TO ROLE DCM_DEVELOPER;
GRANT USAGE ON SCHEMA DCM_DEMO.PROJECTS TO ROLE DCM_DEVELOPER;
GRANT CREATE DCM PROJECT ON SCHEMA DCM_DEMO.PROJECTS TO ROLE DCM_DEVELOPER;

----------------------------------------------------------------------
-- 5. Project object
--
-- The only section that differs between guides: the name is the project_name
-- of the DCM_DEV target in this guide's manifest.yml.
----------------------------------------------------------------------
USE ROLE DCM_DEVELOPER;
CREATE DCM PROJECT IF NOT EXISTS DCM_DEMO.PROJECTS.DCM_PLATFORM_DEV
    COMMENT = 'DEV project of the Build Data Pipelines with Snowflake DCM Projects (platform) quickstart';

----------------------------------------------------------------------
-- 6. Values for the manifest
--
-- Paste ACCOUNT_IDENTIFIER into the account_identifier line of the DCM_DEV
-- target in your guide's manifest.yml (each manifest, if it has two).
----------------------------------------------------------------------
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS ACCOUNT_IDENTIFIER,
       CURRENT_USER() AS USER_NAME;
