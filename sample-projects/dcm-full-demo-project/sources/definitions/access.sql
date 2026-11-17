define warehouse DCM_DEMO_1_WH{{env_suffix}}
with 
    warehouse_size = '{{wh_size}}'
    auto_suspend = 300
    comment = 'Compute for the DCM sample project'
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

grant USAGE on streamlit DCM_DEMO_1{{env_suffix}}.SERVE.DASHBOARD to role DCM_DEMO_1{{env_suffix}}_READ;


-- Inherited grants and container-level MANAGE GRANTS (generally available) require the following account-level opt-in:
-- ALTER ACCOUNT SET FEATURE_RBAC_INHERITED_GRANTS = 'ENABLED';
grant INHERITED SELECT on all tables         in schema DCM_DEMO_1{{env_suffix}}.SERVE to role DCM_DEMO_1{{env_suffix}}_READ;
grant INHERITED SELECT on all dynamic tables in schema DCM_DEMO_1{{env_suffix}}.SERVE to role DCM_DEMO_1{{env_suffix}}_READ;
grant INHERITED SELECT on all views          in schema DCM_DEMO_1{{env_suffix}}.SERVE to role DCM_DEMO_1{{env_suffix}}_READ;
grant INHERITED SELECT on all semantic views in schema DCM_DEMO_1{{env_suffix}}.SERVE to role DCM_DEMO_1{{env_suffix}}_READ;


-- transfer procedure ownership to lower role to avoid running with elevated privileges
define role DCM_DEMO_1{{env_suffix}}_PIPELINE_RUNNER;
grant role DCM_DEMO_1{{env_suffix}}_PIPELINE_RUNNER to role {{project_owner_role}};
grant role DCM_DEMO_1{{env_suffix}}_READ to role DCM_DEMO_1{{env_suffix}}_PIPELINE_RUNNER;

grant ownership on procedure DCM_DEMO_1{{env_suffix}}.RAW.SP_UPSERT_FROM_INCOMING(string, string, string) to role DCM_DEMO_1{{env_suffix}}_PIPELINE_RUNNER;


-- Regional sales roles for the CITY_ROW_FILTER row access policy
define role DCM_DEMO_1_SALES_NA{{env_suffix}};
define role DCM_DEMO_1_SALES_EU{{env_suffix}};

grant role DCM_DEMO_1{{env_suffix}}_READ to role DCM_DEMO_1_SALES_NA{{env_suffix}};
grant role DCM_DEMO_1{{env_suffix}}_READ to role DCM_DEMO_1_SALES_EU{{env_suffix}};


-- Database role scoped to DCM_DEMO_1, for consumers of the semantic view only
define database role DCM_DEMO_1{{env_suffix}}.SEMANTIC_READER
       comment = 'Read access to the orders semantic view'
;

grant USAGE on schema DCM_DEMO_1{{env_suffix}}.SERVE to database role DCM_DEMO_1{{env_suffix}}.SEMANTIC_READER;
grant SELECT on semantic view DCM_DEMO_1{{env_suffix}}.SERVE.ORDERS_SEMANTIC_VIEW to database role DCM_DEMO_1{{env_suffix}}.SEMANTIC_READER;
grant database role DCM_DEMO_1{{env_suffix}}.SEMANTIC_READER to role DCM_DEMO_1_SALES_NA{{env_suffix}};
