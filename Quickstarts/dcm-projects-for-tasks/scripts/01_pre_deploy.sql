/*=============================================================================
  01_pre_deploy.sql: run BEFORE the first DCM Plan and Deploy

  Creates the DCM developer role, the account-level privileges a task graph
  needs, and the DCM project object that manifest.yml points at. Ends with the
  account identifier you paste into manifest.yml.
=============================================================================*/

----------------------------------------------------------------------
-- 1. Create the DCM developer role
----------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS DCM_DEVELOPER
    COMMENT = 'Owns and deploys the DCM projects in the DCM quickstart series';
SET USER_NAME = (SELECT CURRENT_USER());
GRANT ROLE DCM_DEVELOPER TO USER IDENTIFIER($USER_NAME);

----------------------------------------------------------------------
-- 2. Grant account-level privileges
--
-- A project cannot grant account-level privileges to its own owner, so
-- they are granted here, before the first plan.
----------------------------------------------------------------------
-- The project defines a database and a warehouse, both account-level objects.
GRANT CREATE DATABASE  ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Run the task graph. EXECUTE TASK covers tasks and task graphs the role owns.
-- EXECUTE MANAGED TASK is only needed if you switch the members to serverless.
GRANT EXECUTE TASK         ON ACCOUNT TO ROLE DCM_DEVELOPER;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE DCM_DEVELOPER;

-- Read task history for every graph run.
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE DCM_DEVELOPER;

----------------------------------------------------------------------
-- 3. Create the DCM project object
--
-- If DCM_DEMO already exists and another role owns it, CREATE DATABASE
-- fails ("already exists, but current role has no privileges on it").
-- Then run these as ACCOUNTADMIN, skip CREATE DATABASE and CREATE SCHEMA,
-- and run USE ROLE, CREATE DCM PROJECT and section 4:
--   GRANT USAGE ON DATABASE DCM_DEMO TO ROLE DCM_DEVELOPER;
--   GRANT USAGE ON SCHEMA DCM_DEMO.PROJECTS TO ROLE DCM_DEVELOPER;
--   GRANT CREATE DCM PROJECT ON SCHEMA DCM_DEMO.PROJECTS TO ROLE DCM_DEVELOPER;
----------------------------------------------------------------------
USE ROLE DCM_DEVELOPER;

CREATE DATABASE IF NOT EXISTS DCM_DEMO
    COMMENT = 'Holds the DCM project objects of the DCM quickstart series';
CREATE SCHEMA IF NOT EXISTS DCM_DEMO.PROJECTS
    COMMENT = 'DCM project objects';

CREATE DCM PROJECT IF NOT EXISTS DCM_DEMO.PROJECTS.DCM_TASKS_PROJECT_DEV
    COMMENT = 'DCM Projects for Tasks quickstart: one task graph managed as code';

----------------------------------------------------------------------
-- 4. Get your account identifier (paste it into manifest.yml, line 11)
----------------------------------------------------------------------
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS ACCOUNT_IDENTIFIER;
