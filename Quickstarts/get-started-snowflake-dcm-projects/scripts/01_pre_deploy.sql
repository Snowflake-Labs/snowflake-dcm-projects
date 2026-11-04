/*=============================================================================
  01_pre_deploy.sql: Get Started with Snowflake DCM Projects
  Run once, BEFORE the first plan, as ACCOUNTADMIN (the script switches roles).

  Creates the DCM_DEVELOPER role, turns on inherited grants, grants the role the
  account-level privileges a project owner needs, and creates the shared
  DCM_DEMO.PROJECTS schema that holds the project object.

  The role gets the SUPERSET of privileges used by any guide in this series, so
  you can run all of them without re-running this script. Each grant carries a
  comment saying what capability it buys.

  The DCM Project object itself is created by the first plan: select the
  DCM_DEV target and click Plan, and Snowsight asks you to confirm creating it.
=============================================================================*/

----------------------------------------------------------------------
-- 1. Role
----------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS DCM_DEVELOPER
    COMMENT = 'Owns the DCM Projects Quickstart projects and the objects they deploy';
SET MY_USER = CURRENT_USER();
GRANT ROLE DCM_DEVELOPER TO USER IDENTIFIER($MY_USER);
GRANT ROLE DCM_DEVELOPER TO ROLE SYSADMIN;

----------------------------------------------------------------------
-- 2. Inherited grants opt-in
--
-- A one-time, account-wide opt-in, independent of DCM and required before any
-- GRANT INHERITED statement succeeds. An inherited grant covers current and
-- future objects of a type in a container, and cannot be combined with
-- WITH GRANT OPTION, CASCADE or RESTRICT.
----------------------------------------------------------------------
ALTER ACCOUNT SET FEATURE_RBAC_INHERITED_GRANTS = 'ENABLED';

----------------------------------------------------------------------
-- 3. Account-level privileges
--
-- A DCM project cannot grant account-level privileges to its own owner, so
-- they all happen here, before the first plan. Several are checked at PLAN
-- time, not only at deploy: if a definition needs one, the first plan fails.
----------------------------------------------------------------------

-- Create the objects a project manages. Databases, warehouses and roles are
-- account-level objects.
GRANT CREATE DATABASE  ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT CREATE ROLE      ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Grant on objects the owner does not own, which is what makes GRANT statements
-- inside a project work.
GRANT MANAGE GRANTS ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Run tasks. EXECUTE MANAGED TASK is additionally required for serverless tasks,
-- which have no warehouse of their own.
GRANT EXECUTE TASK         ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Run alerts. Same split as tasks: EXECUTE MANAGED ALERT additionally for
-- serverless alerts. An alert that deploys but never fires is usually missing one.
GRANT EXECUTE ALERT         ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT EXECUTE MANAGED ALERT ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Create integrations: the notification integrations DCM cannot define, and the
-- API, external access and storage integrations that later guides define.
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- See task and pipe history across the account: "did it run, and did it succeed".
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Attach data quality expectations, and read their results.
GRANT EXECUTE DATA METRIC FUNCTION ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT APPLICATION ROLE SNOWFLAKE.DATA_QUALITY_MONITORING_VIEWER TO ROLE DCM_DEVELOPER;

-- Turn data quality notifications on for a database.
GRANT MANAGE DATA QUALITY ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Tag-based masking: ALTER TAG ... SET MASKING POLICY, used by the governance
-- guide. Tagging an object the role owns, as this guide does, needs no extra
-- privilege.
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Create network policies, and activate them (ATTACH POLICY).
GRANT CREATE NETWORK POLICY ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT ATTACH POLICY         ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT APPLICATION ROLE SNOWFLAKE.NETWORK_SECURITY_ADMIN TO ROLE DCM_DEVELOPER;

-- Attach an authentication policy to the account or a user.
GRANT APPLY AUTHENTICATION POLICY ON ACCOUNT TO ROLE DCM_DEVELOPER;

----------------------------------------------------------------------
-- 4. Series container
--
-- DCM_DEMO.PROJECTS holds the project objects of every guide in this series.
-- A project cannot define the database or schema it lives in, so they are
-- created here, still as ACCOUNTADMIN. DCM_DEVELOPER needs USAGE on both and
-- CREATE DCM PROJECT on the schema to create and own the project object.
----------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS DCM_DEMO
    COMMENT = 'Shared container for the DCM Projects Quickstart project objects';
CREATE SCHEMA IF NOT EXISTS DCM_DEMO.PROJECTS
    COMMENT = 'Holds the DCM Project objects of every guide in the series';
GRANT USAGE ON DATABASE DCM_DEMO TO ROLE DCM_DEVELOPER;
GRANT USAGE ON SCHEMA DCM_DEMO.PROJECTS TO ROLE DCM_DEVELOPER;
GRANT CREATE DCM PROJECT ON SCHEMA DCM_DEMO.PROJECTS TO ROLE DCM_DEVELOPER;

----------------------------------------------------------------------
-- 5. Objects the project cannot own: none in this guide.
----------------------------------------------------------------------

----------------------------------------------------------------------
-- 6. Project object (alternative path only)
--
-- The first plan creates it for you. Uncomment only if you would rather create
-- it yourself before planning; run it as DCM_DEVELOPER, the owner in the
-- manifest.
----------------------------------------------------------------------
-- USE ROLE DCM_DEVELOPER;
-- CREATE DCM PROJECT IF NOT EXISTS DCM_DEMO.PROJECTS.DCM_PROJECT_DEV
--     COMMENT = 'for testing DCM Projects Quickstarts';

----------------------------------------------------------------------
-- 7. Values for the manifest
--
-- Paste ACCOUNT_IDENTIFIER into the account_identifier line of the DCM_DEV
-- target in DCM_Projects_Get_Started/manifest.yml.
----------------------------------------------------------------------
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS ACCOUNT_IDENTIFIER,
       CURRENT_USER() AS USER_NAME;
