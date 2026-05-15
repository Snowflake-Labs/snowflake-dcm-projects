-- =============================================================================
-- Script 5: Cleanup
-- Run this when you're done and want to tear everything down
-- =============================================================================

-- PURGE drops every object each project created: databases, warehouses, roles,
-- grants, tables, dynamic tables, views — everything managed by the project.
--
-- Run Pipeline's PURGE first to clean out its silver/gold DTs while its host
-- database still exists; Platform's PURGE then drops the databases, warehouses,
-- and roles (including the database that held the Pipeline project object).

-- Pipeline first: purge DTs, views, and DMF attachments inside dcm_demo_2_finance_dev
USE ROLE dcm_demo_2_finance_dev_admin;
EXECUTE DCM PROJECT dcm_demo_2_finance_dev.projects.finance_pipeline PURGE;

-- Platform: drops dcm_demo_2_finance_dev, dcm_demo_2_marketing_dev, dcm_demo_2_dev,
-- warehouses, and all team roles. The Pipeline project object goes with the Finance DB.
USE ROLE dcm_developer;
EXECUTE DCM PROJECT dcm_demo.projects.dcm_platform_dev PURGE;

-- Platform project object itself (in the shared dcm_demo DB), plus the scaffolding
DROP DCM PROJECT IF EXISTS dcm_demo.projects.dcm_platform_dev;
DROP SCHEMA IF EXISTS dcm_demo.projects;
DROP DATABASE IF EXISTS dcm_demo;

USE ROLE ACCOUNTADMIN;
DROP ROLE IF EXISTS dcm_developer;
