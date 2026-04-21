/*=============================================================================
  04_cleanup.sql — Tear down all objects created by this quickstart
=============================================================================*/

USE ROLE dcm_developer;

DROP DATABASE IF EXISTS dcm_demo_3_dev;
DROP WAREHOUSE IF EXISTS dcm_demo_3_wh_dev;

DROP ROLE IF EXISTS dcm_demo_3_dev_read;
DROP ROLE IF EXISTS dev_team_1_owner_dev;
DROP ROLE IF EXISTS dev_team_1_developer_dev;
DROP ROLE IF EXISTS dev_team_1_usage_dev;

USE ROLE ACCOUNTADMIN;
DROP DCM PROJECT IF EXISTS dcm_demo.projects.dcm_project_dev;
DROP SCHEMA IF EXISTS dcm_demo.projects;
DROP DATABASE IF EXISTS dcm_demo;

DROP ROLE IF EXISTS dcm_developer;
DROP WAREHOUSE IF EXISTS dcm_wh;
