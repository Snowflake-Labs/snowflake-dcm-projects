define warehouse DCM_DEMO_1_WH{{env_suffix}}
with 
    warehouse_size = '{{wh_size}}'
    auto_suspend = 300
    comment = 'For Quickstart Demo of DCM Projects PrPr'
;

define database role DCM_DEMO_1{{env_suffix}}.ADMIN{{env_suffix}};
grant database role DCM_DEMO_1{{env_suffix}}.ADMIN{{env_suffix}} to role {{project_owner_role}};
define role DCM_DEMO_1{{env_suffix}}_READ;

grant USAGE on database DCM_DEMO_1{{env_suffix}}         to role DCM_DEMO_1{{env_suffix}}_READ;
grant USAGE on schema DCM_DEMO_1{{env_suffix}}.RAW       to role DCM_DEMO_1{{env_suffix}}_READ;
grant USAGE on schema DCM_DEMO_1{{env_suffix}}.ANALYTICS to role DCM_DEMO_1{{env_suffix}}_READ;
grant USAGE on schema DCM_DEMO_1{{env_suffix}}.SERVE     to role DCM_DEMO_1{{env_suffix}}_READ;
grant USAGE on warehouse DCM_DEMO_1_WH{{env_suffix}}     to role DCM_DEMO_1{{env_suffix}}_READ;

-- Inherited grants require the following account-level opt-in (Public Preview),
-- enabled in scripts/01_pre_deploy.sql:
-- ALTER ACCOUNT SET FEATURE_RBAC_INHERITED_GRANTS = 'ENABLED';
grant INHERITED SELECT on all tables in database DCM_DEMO_1{{env_suffix}}    to role DCM_DEMO_1{{env_suffix}}_READ;
grant INHERITED SELECT on all dynamic tables in database DCM_DEMO_1{{env_suffix}}    to role DCM_DEMO_1{{env_suffix}}_READ;
grant INHERITED SELECT on all views in database DCM_DEMO_1{{env_suffix}}    to role DCM_DEMO_1{{env_suffix}}_READ;