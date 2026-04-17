-- =============================================================================
-- Script 5: Cleanup
-- Run this when you're done and want to tear everything down
-- =============================================================================

USE ROLE dcm_developer;

-- Drop deployed infrastructure from the Pipeline project (inside Finance DB)
DROP DCM PROJECT IF EXISTS dcm_demo_2_finance_dev.projects.finance_pipeline;

-- Drop deployed infrastructure from the Platform project
DROP DATABASE IF EXISTS dcm_demo_2_finance_dev;
DROP DATABASE IF EXISTS dcm_demo_2_marketing_dev;
DROP DATABASE IF EXISTS dcm_demo_2_dev;
DROP WAREHOUSE IF EXISTS dcm_demo_2_finance_wh_dev;
DROP WAREHOUSE IF EXISTS dcm_demo_2_marketing_wh_dev;

-- Drop roles created by the deployments
DROP ROLE IF EXISTS dcm_demo_2_finance_dev_admin;
DROP ROLE IF EXISTS dcm_demo_2_finance_dev_usage;
DROP ROLE IF EXISTS dcm_demo_2_marketing_dev_admin;
DROP ROLE IF EXISTS dcm_demo_2_marketing_dev_usage;

-- Drop DCM Platform Project object
USE ROLE ACCOUNTADMIN;
DROP DCM PROJECT IF EXISTS dcm_demo.projects.dcm_platform_dev;
DROP SCHEMA IF EXISTS dcm_demo.projects;
DROP DATABASE IF EXISTS dcm_demo;

-- Drop the DCM Developer role and warehouse (optional)
DROP ROLE IF EXISTS dcm_developer;
DROP WAREHOUSE IF EXISTS dcm_wh;
