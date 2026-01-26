define warehouse DCM_PROJECT_WH_{{env_suffix}}
with 
    warehouse_size = '{{wh_size}}'
    auto_suspend = 300
    comment = 'For Quickstart Demo of DCM Projects PrPr'
;

define database role DCM_PROJECT_{{env_suffix}}.{{env_suffix}}_ADMIN;
grant database role DCM_PROJECT_{{env_suffix}}.{{env_suffix}}_ADMIN to role {{project_owner_role}};

define role DCM_PROJECT_{{env_suffix}}_READ;

{% for user_name in users %}
    grant role DCM_PROJECT_{{env_suffix}}_READ to user {{user_name}};   
{% endfor %}

grant USAGE on database DCM_PROJECT_{{env_suffix}}         to role DCM_PROJECT_{{env_suffix}}_READ;
grant USAGE on schema DCM_PROJECT_{{env_suffix}}.RAW       to role DCM_PROJECT_{{env_suffix}}_READ;
grant USAGE on schema DCM_PROJECT_{{env_suffix}}.ANALYTICS to role DCM_PROJECT_{{env_suffix}}_READ;
grant USAGE on schema DCM_PROJECT_{{env_suffix}}.SERVE     to role DCM_PROJECT_{{env_suffix}}_READ;
grant USAGE on warehouse DCM_PROJECT_WH_{{env_suffix}}     to role DCM_PROJECT_{{env_suffix}}_READ;


grant SELECT on ALL tables in database DCM_PROJECT_{{env_suffix}}    to role DCM_PROJECT_{{env_suffix}}_READ;
grant SELECT on ALL dynamic tables in database DCM_PROJECT_{{env_suffix}}    to role DCM_PROJECT_{{env_suffix}}_READ;
grant SELECT on ALL views in database DCM_PROJECT_{{env_suffix}}    to role DCM_PROJECT_{{env_suffix}}_READ;