{% macro create_team_roles(team) %}
    
    define role DCM_DEMO_2_{{team}}{{env_suffix}}_ADMIN;
    define role DCM_DEMO_2_{{team}}{{env_suffix}}_USAGE;

    grant CREATE SCHEMA on database DCM_DEMO_2_{{team}}{{env_suffix}} to role DCM_DEMO_2_{{team}}{{env_suffix}}_ADMIN;
    grant USAGE on warehouse DCM_DEMO_2_{{team}}_WH{{env_suffix}} to role DCM_DEMO_2_{{team}}{{env_suffix}}_USAGE;
    grant USAGE on database DCM_DEMO_2_{{team}}{{env_suffix}} to role DCM_DEMO_2_{{team}}{{env_suffix}}_USAGE;
    grant USAGE on schema DCM_DEMO_2_{{team}}{{env_suffix}}.PROJECTS to role DCM_DEMO_2_{{team}}{{env_suffix}}_USAGE;
    grant CREATE DCM PROJECT on schema DCM_DEMO_2_{{team}}{{env_suffix}}.PROJECTS to role DCM_DEMO_2_{{team}}{{env_suffix}}_ADMIN;
    
    grant role DCM_DEMO_2_{{team}}{{env_suffix}}_USAGE to role DCM_DEMO_2_{{team}}{{env_suffix}}_ADMIN;
    grant role DCM_DEMO_2_{{team}}{{env_suffix}}_ADMIN to role {{project_owner_role}};


    {% for user_name in users %}
        grant role DCM_DEMO_2_{{team}}{{env_suffix}}_USAGE to user {{user_name}};
    {% endfor %}
{% endmacro %}