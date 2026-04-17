-- =============================================================================
-- Script 1: Pre-Deploy Setup
-- Run this BEFORE your first DCM Plan & Deploy
-- =============================================================================

-- 1. Create a DCM Developer Role
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS dcm_developer;
SET user_name = (SELECT CURRENT_USER());
GRANT ROLE dcm_developer TO USER IDENTIFIER($user_name);

-- 2. Grant Infrastructure Privileges
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE dcm_developer;
GRANT CREATE ROLE ON ACCOUNT TO ROLE dcm_developer;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE dcm_developer;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE dcm_developer;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE dcm_developer;

GRANT MANAGE GRANTS ON ACCOUNT TO ROLE dcm_developer;

-- 3. Grant Data Quality Privileges
GRANT APPLICATION ROLE SNOWFLAKE.DATA_QUALITY_MONITORING_VIEWER TO ROLE dcm_developer;
GRANT APPLICATION ROLE SNOWFLAKE.DATA_QUALITY_MONITORING_ADMIN TO ROLE dcm_developer;
GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE dcm_developer;
GRANT EXECUTE DATA METRIC FUNCTION ON ACCOUNT TO ROLE dcm_developer WITH GRANT OPTION;

-- 4. Create a Warehouse (optional — skip if you already have one)
USE ROLE dcm_developer;

CREATE WAREHOUSE IF NOT EXISTS dcm_wh
WITH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 300
    COMMENT = 'For Quickstart Demo of DCM Projects';

-- 5. Create the Platform DCM Project Object
CREATE DATABASE IF NOT EXISTS dcm_demo;
CREATE SCHEMA IF NOT EXISTS dcm_demo.projects;

CREATE DCM PROJECT IF NOT EXISTS dcm_demo.projects.dcm_platform_dev
    COMMENT = 'for DCM Platform Demo - Quickstart 2';

-- 6. Get your account identifier and username (needed for the manifest)
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS account_identifier,
       CURRENT_USER() AS user_name;
