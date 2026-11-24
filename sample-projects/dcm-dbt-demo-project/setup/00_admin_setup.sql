-- One-time setup, run by an administrator.
-- Creates the role that deploys the DCM project and runs dbt, and the
-- container for the DCM project objects (a project cannot define its own parent).

use role ACCOUNTADMIN;

create role if not exists DCM_DBT_DEMO_DEPLOYER;
grant role DCM_DBT_DEMO_DEPLOYER to role SYSADMIN;

grant create database on account to role DCM_DBT_DEMO_DEPLOYER;
grant create warehouse on account to role DCM_DBT_DEMO_DEPLOYER;
grant create role on account to role DCM_DBT_DEMO_DEPLOYER;
grant manage grants on account to role DCM_DBT_DEMO_DEPLOYER;
grant apply masking policy on account to role DCM_DBT_DEMO_DEPLOYER;
grant apply row access policy on account to role DCM_DBT_DEMO_DEPLOYER;
grant apply tag on account to role DCM_DBT_DEMO_DEPLOYER;
grant execute task on account to role DCM_DBT_DEMO_DEPLOYER;
grant execute data metric function on account to role DCM_DBT_DEMO_DEPLOYER;
grant database role SNOWFLAKE.DATA_METRIC_USER to role DCM_DBT_DEMO_DEPLOYER;

-- Inherited grants are behind an account-level opt-in until the behavior change rolls out
alter account set FEATURE_RBAC_INHERITED_GRANTS = 'ENABLED';

-- Replace with your user name
set MY_USER = '<your_user_name>';
grant role DCM_DBT_DEMO_DEPLOYER to user IDENTIFIER($MY_USER);

use role DCM_DBT_DEMO_DEPLOYER;

create database if not exists DCM_DBT_DEMO_ADMIN;
create schema if not exists DCM_DBT_DEMO_ADMIN.PROJECTS;

create dcm project if not exists DCM_DBT_DEMO_ADMIN.PROJECTS.DCM_DBT_DEMO_DEV;
create dcm project if not exists DCM_DBT_DEMO_ADMIN.PROJECTS.DCM_DBT_DEMO_PROD;
