/*=============================================================================
  03_cleanup.sql — Tear down everything created by this quickstart.
=============================================================================*/

USE ROLE dcm_developer;

-- Remove the associations applied outside DCM before purge. A network policy
-- can't be dropped while it's assigned to a user, and purge fails on a
-- masking policy that is still associated with a tag.
ALTER USER IF EXISTS dcm_demo_5_dev_svc UNSET NETWORK_POLICY;
ALTER USER IF EXISTS dcm_demo_5_dev_svc UNSET AUTHENTICATION POLICY;
ALTER TAG IF EXISTS dcm_demo_5_dev.gov.pii UNSET MASKING POLICY dcm_demo_5_dev.gov.email_mask;
ALTER TABLE IF EXISTS dcm_demo_5_dev.raw.customer DROP ROW ACCESS POLICY dcm_demo_5_dev.gov.customer_country_filter;

-- PURGE drops every object the project manages (db, warehouse, roles, schemas,
-- table, tags, policies) and detaches the masking policy it attached with
-- ATTACH MASKING POLICY.
EXECUTE DCM PROJECT dcm_demo.projects.dcm_gov_project_dev PURGE;

DROP DCM PROJECT IF EXISTS dcm_demo.projects.dcm_gov_project_dev;
DROP USER IF EXISTS dcm_demo_5_dev_svc;

USE ROLE ACCOUNTADMIN;
DROP ROLE IF EXISTS dcm_demo_5_dev_analyst;
