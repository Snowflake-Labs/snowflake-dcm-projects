-- Manual administrator setup for both sample projects. DCM does not run this file.
-- Runs in one go on a single account that hosts both the DEV and PROD targets.
-- It creates the DEV and PROD deployer roles and grants them broad demo-deployer privileges,
-- including account-wide grant administration. It does not assign users or deploy projects.

use role ACCOUNTADMIN;

-- Account-wide opt-in required for inherited grants and container-level MANAGE GRANTS.
alter account set FEATURE_RBAC_INHERITED_GRANTS = 'ENABLED';


create database if not exists DCM_DEMO;
create schema if not exists DCM_DEMO.PROJECTS;
create warehouse if not exists DCM_WH
    with
    warehouse_size = 'XSMALL'
    auto_suspend = 600
;

create role if not exists DCM_DEVELOPER;
create role if not exists DCM_PROD_DEPLOYER;



-- DEV: DCM_DEV target
grant usage on database DCM_DEMO to role DCM_DEVELOPER;
grant 
    usage, 
    create dcm project, 
    create stage 
        on schema DCM_DEMO.PROJECTS to role DCM_DEVELOPER;

grant usage, monitor on warehouse DCM_WH to role DCM_DEVELOPER;

grant 
    create warehouse, 
    create role, 
    create database, 
    create network policy, 
    create integration,
    create share,
    apply tag,
    modify log level
	    on account to role DCM_DEVELOPER;

grant 
    execute task,
    execute alert,
    execute data metric function 
        on account to role DCM_DEVELOPER 
    with grant option;

-- Delegation is required by the container grants in access.sql and grants_macro.sql.
grant MANAGE GRANTS on account to role DCM_DEVELOPER 
    with grant option;

grant application role SNOWFLAKE.NETWORK_SECURITY_ADMIN to role DCM_DEVELOPER;
grant usage on compute pool SYSTEM_COMPUTE_POOL_CPU to role DCM_DEVELOPER;

-- Runtime access for the existing AI_COMPLETE expression in analytics.sql.
grant database role SNOWFLAKE.CORTEX_USER to role DCM_DEVELOPER;
grant use AI FUNCTIONS on account to role DCM_DEVELOPER;



-- PROD: DCM_PROD_US target
grant usage on database DCM_DEMO to role DCM_PROD_DEPLOYER;
grant 
    usage, 
    create dcm project, 
    create stage 
        on schema DCM_DEMO.PROJECTS to role DCM_PROD_DEPLOYER;

grant usage, monitor on warehouse DCM_WH to role DCM_PROD_DEPLOYER;

grant 
    create warehouse, 
    create role, 
    create database, 
    create network policy, 
    create integration,
    create share,
    apply tag,
    modify log level
	    on account to role DCM_PROD_DEPLOYER;

grant 
    execute task,
    execute alert,
    execute data metric function 
        on account to role DCM_PROD_DEPLOYER 
    with grant option;

grant MANAGE GRANTS on account to role DCM_PROD_DEPLOYER 
    with grant option;

grant application role SNOWFLAKE.NETWORK_SECURITY_ADMIN to role DCM_PROD_DEPLOYER;
grant usage on compute pool SYSTEM_COMPUTE_POOL_CPU to role DCM_PROD_DEPLOYER;

grant database role SNOWFLAKE.CORTEX_USER to role DCM_PROD_DEPLOYER;
grant use AI FUNCTIONS on account to role DCM_PROD_DEPLOYER;
