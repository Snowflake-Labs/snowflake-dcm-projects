/*=============================================================================
  01_pre_deploy.sql — Run BEFORE the first DCM Plan & Deploy

  Creates the DCM Developer role, grants, the account opt-in for inherited
  grants, and the DCM Project object that the manifest references.

  The role is granted the SUPERSET of privileges needed by any guide in this
  series, so you can run all of them without re-running this script. Every
  grant this guide does not itself use carries a comment naming the guide that
  does need it.
=============================================================================*/

----------------------------------------------------------------------
-- 1. Create a DCM Developer Role
----------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS dcm_developer;
SET user_name = (SELECT CURRENT_USER());
GRANT ROLE dcm_developer TO USER IDENTIFIER($user_name);

----------------------------------------------------------------------
-- 2. Enable inherited grants
--
-- An account-level opt-in, independent of DCM and required before any
-- GRANT INHERITED statement will succeed. It needs ACCOUNTADMIN and applies
-- account-wide. Inherited grants replace GRANT ON ALL / GRANT ON FUTURE and
-- cannot be combined with WITH GRANT OPTION, CASCADE or RESTRICT.
----------------------------------------------------------------------
ALTER ACCOUNT SET FEATURE_RBAC_INHERITED_GRANTS = 'ENABLED';

----------------------------------------------------------------------
-- 3. Grant account-level privileges
--
-- Account-level privileges cannot be granted by a DCM project on itself, so
-- they all have to happen here, before the first plan. Each block below says
-- what capability the grant buys, not which demo uses it.
--
-- Several of these are needed at PLAN time, not just at run time: if a
-- definition sets a property that requires an account-level privilege, the very
-- first plan fails rather than the deploy.
----------------------------------------------------------------------

-- Create the objects a project manages. Databases, warehouses and roles are
-- account-level, so the project owner needs these to create any of them.
-- MANAGE GRANTS lets the owner grant on objects it does not own, which is what
-- makes GRANT statements inside a project work.
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE dcm_developer;
GRANT CREATE ROLE      ON ACCOUNT TO ROLE dcm_developer;
GRANT CREATE DATABASE  ON ACCOUNT TO ROLE dcm_developer;
GRANT MANAGE GRANTS    ON ACCOUNT TO ROLE dcm_developer;

-- Run scheduled work. EXECUTE TASK lets the owner run tasks it owns;
-- EXECUTE MANAGED TASK is additionally required for serverless tasks, which
-- have no warehouse of their own. A serverless task needs BOTH.
GRANT EXECUTE TASK         ON ACCOUNT TO ROLE dcm_developer;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE dcm_developer;

-- Run alerts. Same split as tasks: EXECUTE ALERT for alerts the owner owns,
-- EXECUTE MANAGED ALERT additionally for serverless ones. An alert that deploys
-- but never fires is usually one of these missing.
GRANT EXECUTE ALERT         ON ACCOUNT TO ROLE dcm_developer;
GRANT EXECUTE MANAGED ALERT ON ACCOUNT TO ROLE dcm_developer;

-- Send email or webhook notifications. Notification integrations are one of the
-- object types DCM cannot DEFINE, so anything that notifies needs the
-- integration created outside the project — and created BEFORE the first plan,
-- because plan validates the reference. Prefer the granular privilege over
-- CREATE INTEGRATION, which grants every integration type.
GRANT CREATE NOTIFICATION INTEGRATION ON ACCOUNT TO ROLE dcm_developer;

-- Emit and read telemetry. Setting LOG_LEVEL or TRACE_LEVEL on a database,
-- schema or object requires these at the ACCOUNT level, so a project whose
-- definitions set them fails at PLAN without them. MODIFY EVENT TABLE is what
-- lets an object be pointed at an event table. Without a log level there are no
-- events to read, so anything that alerts on telemetry silently never fires.
GRANT MODIFY LOG LEVEL   ON ACCOUNT TO ROLE dcm_developer;
GRANT MODIFY TRACE LEVEL ON ACCOUNT TO ROLE dcm_developer;
GRANT MODIFY EVENT TABLE ON ACCOUNT TO ROLE dcm_developer;

-- See what ran. MONITOR EXECUTION covers task and pipe history across the
-- account, which is what you need to answer "did it run, and did it succeed".
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE dcm_developer;

-- Attach and evaluate data quality expectations. EXECUTE DATA METRIC FUNCTION is
-- required to attach a DMF at all. The VIEWER application role is required to
-- READ results from DATA_QUALITY_MONITORING_RESULTS; the ADMIN role is only
-- needed for the raw event table, and every role already has LOOKUP via PUBLIC,
-- so this pair is the minimal set.
GRANT EXECUTE DATA METRIC FUNCTION ON ACCOUNT TO ROLE dcm_developer;
GRANT APPLICATION ROLE SNOWFLAKE.DATA_QUALITY_MONITORING_VIEWER TO ROLE dcm_developer;

-- Turn data quality NOTIFICATIONS on. Setting DATA_QUALITY_MONITORING_SETTINGS
-- on a database requires this at the account level, so a definition that
-- configures it fails at plan without it.
GRANT MANAGE DATA QUALITY ON ACCOUNT TO ROLE dcm_developer;

-- Attach tags and protection policies. The account-level APPLY privileges are
-- what allow a role to attach a tag or a policy to an object it does NOT own,
-- and APPLY MASKING POLICY is additionally what allows a masking policy to be
-- set ON A TAG — the mechanism behind tag-based masking.
--
-- This project attaches one tag to a table it owns, where ownership alone is
-- sufficient. These grants are for the cross-ownership case.
GRANT APPLY TAG               ON ACCOUNT TO ROLE dcm_developer;
GRANT APPLY MASKING POLICY    ON ACCOUNT TO ROLE dcm_developer;
GRANT APPLY ROW ACCESS POLICY ON ACCOUNT TO ROLE dcm_developer;

-- Restrict network access. CREATE NETWORK POLICY creates the policy; ATTACH
-- POLICY is what ACTIVATES it by associating it with the account. Without
-- ATTACH POLICY a network policy can be created but never takes effect.
GRANT CREATE NETWORK POLICY ON ACCOUNT TO ROLE dcm_developer;
GRANT ATTACH POLICY         ON ACCOUNT TO ROLE dcm_developer;

-- Control how users authenticate. APPLY AUTHENTICATION POLICY is needed to
-- attach an authentication policy to the account or to a user; creating one is
-- a schema-level privilege.
GRANT APPLY AUTHENTICATION POLICY ON ACCOUNT TO ROLE dcm_developer;

-- Call Cortex AI functions. Needed by any definition whose body calls an AI
-- function, and by Cortex Analyst when it reads a semantic view.
GRANT USE AI FUNCTIONS ON ACCOUNT TO ROLE dcm_developer;

----------------------------------------------------------------------
-- 4. Create the DCM Project Object
----------------------------------------------------------------------
USE ROLE dcm_developer;

CREATE DATABASE IF NOT EXISTS dcm_demo;
CREATE SCHEMA IF NOT EXISTS dcm_demo.projects;

CREATE OR REPLACE DCM PROJECT dcm_demo.projects.dcm_project_dev
    COMMENT = 'for testing DCM Projects Quickstarts';

----------------------------------------------------------------------
-- 5. Get your account identifier and username
--    (use these values to update manifest.yml)
----------------------------------------------------------------------
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS account_identifier,
       CURRENT_USER() AS user_name;
