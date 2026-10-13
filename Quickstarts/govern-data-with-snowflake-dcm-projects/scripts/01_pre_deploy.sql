/*=============================================================================
  01_pre_deploy.sql — Run BEFORE the first DCM Plan & Deploy.
  Creates the DCM Developer role + grants, enables inherited grants, and
  creates the DCM Project object the manifest references.
=============================================================================*/

USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS dcm_developer;
SET user_name = (SELECT CURRENT_USER());
GRANT ROLE dcm_developer TO USER IDENTIFIER($user_name);

-- Infrastructure privileges
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE dcm_developer;
GRANT CREATE ROLE ON ACCOUNT TO ROLE dcm_developer;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE dcm_developer;
GRANT MANAGE GRANTS ON ACCOUNT TO ROLE dcm_developer;
-- Network policies are account-level objects, so the deploy role needs this:
GRANT CREATE NETWORK POLICY ON ACCOUNT TO ROLE dcm_developer;

-- dcm_developer owns the masking and row access policies it deploys, which is
-- enough to attach them. Setting an authentication policy on a user needs a
-- separate privilege (used in 02_post_deploy.sql):
GRANT APPLY AUTHENTICATION POLICY ON ACCOUNT TO ROLE dcm_developer;

-- A dedicated service user to activate the demo network and authentication
-- policies on. Never activate demo policies on your own user or the account.
-- Owning the user lets dcm_developer set its network policy.
CREATE USER IF NOT EXISTS dcm_demo_5_dev_svc
    TYPE = SERVICE
    COMMENT = 'Demo service user for the Security & Governance Quickstart; target of the demo network and authentication policies';
GRANT OWNERSHIP ON USER dcm_demo_5_dev_svc TO ROLE dcm_developer COPY CURRENT GRANTS;

-- Inherited grants (Public Preview) opt-in - required for the GRANT INHERITED
-- statements in infrastructure.sql. Independent of DCM.
ALTER ACCOUNT SET FEATURE_RBAC_INHERITED_GRANTS = 'ENABLED';

-- Create the DCM Project object
USE ROLE dcm_developer;
CREATE DATABASE IF NOT EXISTS dcm_demo;
CREATE SCHEMA IF NOT EXISTS dcm_demo.projects;

CREATE OR REPLACE DCM PROJECT dcm_demo.projects.dcm_gov_project_dev
    COMMENT = 'for the Security & Governance Quickstart';

-- Account identifier + username for the manifest
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS account_identifier,
       CURRENT_USER() AS user_name;
