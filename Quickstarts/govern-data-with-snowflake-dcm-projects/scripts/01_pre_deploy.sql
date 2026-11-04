/*=============================================================================
  01_pre_deploy.sql: Govern Data with Snowflake DCM Projects
  Run ONCE, as ACCOUNTADMIN, BEFORE the first DCM plan.

  Creates the DCM Developer role and its account-level grants, turns on
  inherited grants, prepares the shared container for project objects, and
  creates the demo service user this guide sets its network and
  authentication policies on. Never set the demo policies on your own user
  or on the account.

  The role is granted the SUPERSET of privileges used by the guides in this
  series, so you can run all of them without re-running this script. Each
  grant carries a comment saying what capability it buys.

  The DCM Project object itself is created by the first plan (see section 6).
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
-- A one-time, account-wide opt-in, independent of DCM, required before any
-- GRANT INHERITED statement succeeds. This project's analyst-role grants use
-- inherited grants. They cannot be combined with WITH GRANT OPTION, CASCADE
-- or RESTRICT.
----------------------------------------------------------------------
ALTER ACCOUNT SET FEATURE_RBAC_INHERITED_GRANTS = 'ENABLED';

----------------------------------------------------------------------
-- 3. Account-level privileges
--
-- A DCM project cannot grant account-level privileges to its own owner, so
-- they all happen here, before the first plan. Some are checked at PLAN
-- time: if a definition needs one, the first plan fails rather than the
-- deploy.
----------------------------------------------------------------------

-- Create the objects a project manages. Databases, warehouses and roles are
-- account-level, so the project owner needs these to create any of them.
-- MANAGE GRANTS lets the owner grant on objects it does not own, which is what
-- makes GRANT statements inside a project work.
GRANT CREATE DATABASE  ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT CREATE ROLE      ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT MANAGE GRANTS    ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Run scheduled work. EXECUTE TASK lets the owner run tasks it owns;
-- EXECUTE MANAGED TASK is additionally required for serverless tasks.
GRANT EXECUTE TASK         ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Run alerts. Same split as tasks: EXECUTE ALERT for alerts the owner owns,
-- EXECUTE MANAGED ALERT additionally for serverless ones.
GRANT EXECUTE ALERT         ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT EXECUTE MANAGED ALERT ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Create integrations. CREATE INTEGRATION covers every integration type:
-- notification integrations, which a project cannot define and must exist
-- before the first plan, and the API, external access and storage
-- integrations that a project can define.
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- See what ran. MONITOR EXECUTION covers task and pipe history across the
-- account.
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Attach and evaluate data quality expectations. EXECUTE DATA METRIC FUNCTION
-- is required to attach a DMF at all; the VIEWER application role is required
-- to read DATA_QUALITY_MONITORING_RESULTS.
GRANT EXECUTE DATA METRIC FUNCTION ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT APPLICATION ROLE SNOWFLAKE.DATA_QUALITY_MONITORING_VIEWER TO ROLE DCM_DEVELOPER;

-- Turn data quality notifications on. Setting DATA_QUALITY_MONITORING_SETTINGS
-- on a database requires this at the account level.
GRANT MANAGE DATA QUALITY ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Governance-specific grants (this guide). The rest of the series does not use
-- them.
--
-- Set a masking policy on a tag (tag-based masking). 02_post_deploy.sql does
-- this with ALTER TAG, which needs the account-level APPLY MASKING POLICY.
-- DCM_DEVELOPER owns the tags, the policies and the table the project deploys,
-- and ownership is all that ATTACH TAG, ATTACH MASKING POLICY and ATTACH ROW
-- ACCESS POLICY need, so this guide grants no APPLY TAG or APPLY ROW ACCESS
-- POLICY. Grant those only if you attach to objects another role owns.
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Restrict network access. Network policies are account-level objects, so
-- creating one needs CREATE NETWORK POLICY. ATTACH POLICY is what would set a
-- network policy on the whole account; this guide only sets it on its demo
-- user, which the role owns.
GRANT CREATE NETWORK POLICY ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT ATTACH POLICY         ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT APPLICATION ROLE SNOWFLAKE.NETWORK_SECURITY_ADMIN TO ROLE DCM_DEVELOPER;

-- Control how users authenticate. APPLY AUTHENTICATION POLICY is needed to set
-- an authentication policy on a user or the account.
GRANT APPLY AUTHENTICATION POLICY ON ACCOUNT TO ROLE DCM_DEVELOPER;

----------------------------------------------------------------------
-- 4. Series container
--
-- Every guide in this series keeps its DCM Project object in
-- DCM_DEMO.PROJECTS.
----------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS DCM_DEMO
    COMMENT = 'Holds the DCM Project objects of the DCM Projects quickstarts';
CREATE SCHEMA IF NOT EXISTS DCM_DEMO.PROJECTS
    COMMENT = 'DCM Project objects of the DCM Projects quickstarts';
GRANT USAGE ON DATABASE DCM_DEMO TO ROLE DCM_DEVELOPER;
GRANT USAGE ON SCHEMA DCM_DEMO.PROJECTS TO ROLE DCM_DEVELOPER;
GRANT CREATE DCM PROJECT ON SCHEMA DCM_DEMO.PROJECTS TO ROLE DCM_DEVELOPER;

----------------------------------------------------------------------
-- 5. Objects the project cannot own: the demo service user
--
-- Users are account-level objects outside any DCM Project. 02_post_deploy.sql
-- sets the network and authentication policies on this user. Owning the user
-- is what lets DCM_DEVELOPER set its network policy.
----------------------------------------------------------------------
CREATE USER IF NOT EXISTS DCM_DEMO_5_DEV_SVC
    TYPE = SERVICE
    COMMENT = 'Demo service user for the Govern Data with DCM Projects quickstart; target of the demo network and authentication policies';
GRANT OWNERSHIP ON USER DCM_DEMO_5_DEV_SVC TO ROLE DCM_DEVELOPER COPY CURRENT GRANTS;

----------------------------------------------------------------------
-- 6. Project object (alternative path only)
--
-- The first plan in Snowsight offers to create the project object from the
-- manifest, and snow dcm create does the same from the CLI. Run this only if
-- the prompt does not appear, or you would rather create it yourself, as
-- DCM_DEVELOPER.
----------------------------------------------------------------------
-- USE ROLE DCM_DEVELOPER;
-- CREATE DCM PROJECT IF NOT EXISTS DCM_DEMO.PROJECTS.DCM_GOV_PROJECT_DEV
--     COMMENT = 'for testing DCM Projects Quickstarts';

----------------------------------------------------------------------
-- 7. Values for the manifest
--
-- Paste ACCOUNT_IDENTIFIER into the account_identifier line of the DCM_DEV
-- target in manifest.yml.
----------------------------------------------------------------------
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS ACCOUNT_IDENTIFIER,
       CURRENT_USER() AS USER_NAME;
