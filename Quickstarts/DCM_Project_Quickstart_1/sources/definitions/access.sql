define warehouse DCM_DEMO_1_WH{{env_suffix}}
with 
    warehouse_size = '{{wh_size}}'
    auto_suspend = 300
    comment = 'For Quickstart Demo of DCM Projects PrPr'
;

define role DCM_DEMO_1_ADMIN{{env_suffix}};

grant role DCM_DEMO_1_ADMIN{{env_suffix}} to role {{project_owner_role}};
-- ensures that the DCM project owner still holds all roles to avoid lock-out 

define role DCM_DEMO_1{{env_suffix}}_READ;
grant role DCM_DEMO_1{{env_suffix}}_READ to user {{user_name}};   

grant USAGE on warehouse DCM_DEMO_1_WH{{env_suffix}}     to role DCM_DEMO_1{{env_suffix}}_READ;
grant USAGE on database DCM_DEMO_1{{env_suffix}}         to role DCM_DEMO_1{{env_suffix}}_READ;
grant USAGE on schema DCM_DEMO_1{{env_suffix}}.RAW       to role DCM_DEMO_1{{env_suffix}}_READ;
grant USAGE on schema DCM_DEMO_1{{env_suffix}}.ANALYTICS to role DCM_DEMO_1{{env_suffix}}_READ;
grant USAGE on schema DCM_DEMO_1{{env_suffix}}.SERVE     to role DCM_DEMO_1{{env_suffix}}_READ;


-- Inherited grants and container-level MANAGE GRANTS require the following account-level opt-in (Public Preview):
-- ALTER ACCOUNT SET FEATURE_RBAC_INHERITED_GRANTS = 'ENABLED';

grant INHERITED SELECT on all tables         in schema DCM_DEMO_1{{env_suffix}}.SERVE to role DCM_DEMO_1{{env_suffix}}_READ;
grant INHERITED SELECT on all dynamic tables in schema DCM_DEMO_1{{env_suffix}}.SERVE to role DCM_DEMO_1{{env_suffix}}_READ;
grant INHERITED SELECT on all views          in schema DCM_DEMO_1{{env_suffix}}.SERVE to role DCM_DEMO_1{{env_suffix}}_READ;
grant INHERITED USAGE on all streamlits      in schema DCM_DEMO_1{{env_suffix}}.SERVE to role DCM_DEMO_1{{env_suffix}}_READ;
grant INHERITED SELECT on all semantic views  in schema DCM_DEMO_1{{env_suffix}}.SERVE to role DCM_DEMO_1{{env_suffix}}_READ;
