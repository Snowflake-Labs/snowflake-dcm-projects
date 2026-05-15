/*=============================================================================
  03_cleanup.sql — Run when you're done and want to tear everything down
=============================================================================*/

-- PURGE drops every object the project created: databases, warehouses, roles,
-- grants, tables, dynamic tables, views — everything managed by the project.
USE ROLE dcm_developer;
EXECUTE DCM PROJECT dcm_demo.projects.dcm_project_dev PURGE;

DROP DCM PROJECT IF EXISTS dcm_demo.projects.dcm_project_dev;
DROP SCHEMA IF EXISTS dcm_demo.projects;
DROP DATABASE IF EXISTS dcm_demo;

USE ROLE ACCOUNTADMIN;
DROP ROLE IF EXISTS dcm_developer;
