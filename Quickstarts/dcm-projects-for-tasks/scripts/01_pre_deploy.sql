/*=============================================================================
  01_pre_deploy.sql — Run BEFORE the first DCM Plan & Deploy

  Creates the DCM Developer role, grants, warehouse, DCM Project object,
  and an email Notification Integration used by the task graph's finalizer
  for summary emails and by post-deploy alerts.
=============================================================================*/

----------------------------------------------------------------------
-- 1. Create a DCM Developer Role
----------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS dcm_developer;
SET user_name = (SELECT CURRENT_USER());
GRANT ROLE dcm_developer TO USER IDENTIFIER($user_name);

----------------------------------------------------------------------
-- 2. Grant Infrastructure Privileges
----------------------------------------------------------------------
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE dcm_developer;
GRANT CREATE ROLE ON ACCOUNT TO ROLE dcm_developer;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE dcm_developer;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE dcm_developer;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE dcm_developer;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE dcm_developer;
GRANT EXECUTE ALERT ON ACCOUNT TO ROLE dcm_developer;
GRANT EXECUTE MANAGED ALERT ON ACCOUNT TO ROLE dcm_developer;
GRANT MANAGE GRANTS ON ACCOUNT TO ROLE dcm_developer;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE dcm_developer;

----------------------------------------------------------------------
-- 3. Grant Data Quality Privileges (for the DMF quality-gate branch)
----------------------------------------------------------------------
GRANT APPLICATION ROLE SNOWFLAKE.DATA_QUALITY_MONITORING_VIEWER TO ROLE dcm_developer;
GRANT APPLICATION ROLE SNOWFLAKE.DATA_QUALITY_MONITORING_ADMIN  TO ROLE dcm_developer;
GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE dcm_developer;
GRANT EXECUTE DATA METRIC FUNCTION ON ACCOUNT TO ROLE dcm_developer;

----------------------------------------------------------------------
-- 4. Create a Warehouse (optional — skip if you already have one)
----------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS dcm_wh
WITH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 300
    COMMENT = 'For Quickstart Demo of DCM Projects';

----------------------------------------------------------------------
-- 5. Create the DCM Project Object
----------------------------------------------------------------------
USE ROLE dcm_developer;

CREATE DATABASE IF NOT EXISTS dcm_demo;
CREATE SCHEMA IF NOT EXISTS dcm_demo.projects;

CREATE OR REPLACE DCM PROJECT dcm_demo.projects.dcm_project_dev
    COMMENT = 'for testing DCM Projects with Tasks and task graphs';

----------------------------------------------------------------------
-- 6. Create the Email Notification Integration
--    (account-level object — lives outside the DCM Project)
--    The finalizer task and post-deploy alerts both send to it.
--    Replace the recipient with your verified Snowflake user email.
----------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE NOTIFICATION INTEGRATION IF NOT EXISTS dcm_demo_email_notifications
    TYPE = EMAIL
    ENABLED = TRUE
    COMMENT = 'Used by DCM Tasks quickstart finalizer and alerts';

GRANT USAGE ON INTEGRATION dcm_demo_email_notifications TO ROLE dcm_developer;

-- Test it (optional) — replace with your verified user email
-- CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
--     SNOWFLAKE.NOTIFICATION.TEXT_PLAIN('Hello from Snowflake!'),
--     SNOWFLAKE.NOTIFICATION.EMAIL_INTEGRATION_CONFIG(
--         'dcm_demo_email_notifications',
--         'DCM Quickstart Test',
--         ARRAY_CONSTRUCT('your_email@example.com'),
--         NULL, NULL));

----------------------------------------------------------------------
-- 7. Get your account identifier and username (to update manifest.yml
--    and the notification_recipient value for the DEV target)
----------------------------------------------------------------------
SELECT
    CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS account_identifier,
    CURRENT_USER() AS user_name;
