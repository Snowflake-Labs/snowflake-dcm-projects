/*=============================================================================
  03_cleanup.sql — Tear down everything created by this quickstart.
=============================================================================*/

USE ROLE dcm_developer;

-- Remove the tag<->masking-policy association before purge
ALTER TAG IF EXISTS dcm_demo_5_dev.gov.pii UNSET MASKING POLICY dcm_demo_5_dev.gov.email_mask;

-- PURGE drops every object the project manages (db, warehouse, roles, schemas,
-- table, tags, policies).
EXECUTE DCM PROJECT dcm_demo.projects.dcm_gov_project_dev PURGE;

DROP DCM PROJECT IF EXISTS dcm_demo.projects.dcm_gov_project_dev;

USE ROLE ACCOUNTADMIN;
DROP ROLE IF EXISTS dcm_demo_5_dev_analyst;
