{% for team in teams %}
    {% set team_name = team.name | upper %}
    define warehouse DCM_DEMO_2_{{team_name}}_WH{{env_suffix}}
        with warehouse_size='{{wh_size}}'
        comment = 'For DCM Demo Quickstart 2';      
        
    define database DCM_DEMO_2_{{team_name}}{{env_suffix}};
    define schema DCM_DEMO_2_{{team_name}}{{env_suffix}}.PROJECTS;
    define schema DCM_DEMO_2_{{team_name}}{{env_suffix}}.ANALYTICS;
        

    {{ create_team_roles(team_name) }}

    {% if team.raw_access == 'READ' %}
        grant USAGE on database DCM_DEMO_2{{env_suffix}} to role DCM_DEMO_2_{{team_name}}{{env_suffix}}_ADMIN;
        grant USAGE on schema DCM_DEMO_2{{env_suffix}}.RAW to role DCM_DEMO_2_{{team_name}}{{env_suffix}}_ADMIN;
        grant select on ALL TABLES in schema DCM_DEMO_2{{env_suffix}}.RAW to role DCM_DEMO_2_{{team_name}}{{env_suffix}}_ADMIN;    
        
    {% elif team.raw_access == 'WRITE' %}
        grant USAGE on database DCM_DEMO_2{{env_suffix}} to role DCM_DEMO_2_{{team_name}}{{env_suffix}}_ADMIN;
        grant USAGE on schema DCM_DEMO_2{{env_suffix}}.RAW to role DCM_DEMO_2_{{team_name}}{{env_suffix}}_ADMIN;
        grant select on ALL TABLES in schema DCM_DEMO_2{{env_suffix}}.RAW to role DCM_DEMO_2_{{team_name}}{{env_suffix}}_ADMIN;
        grant insert, update, delete on ALL TABLES in schema DCM_DEMO_2{{env_suffix}}.RAW to role DCM_DEMO_2_{{team_name}}{{env_suffix}}_ADMIN;
    {% endif %}

    {% if team_name == 'FINANCE' %}
        -- grant application role SNOWFLAKE.DATA_QUALITY_MONITORING_VIEWER to role DCM_DEMO_2_{{team_name}}_ADMIN;       -- application roles are not yet supported in DCM Projects
        -- grant application role SNOWFLAKE.DATA_QUALITY_MONITORING_ADMIN to role DCM_DEMO_2_{{team_name}}_ADMIN;
        grant database role SNOWFLAKE.DATA_METRIC_USER to role DCM_DEMO_2_{{team_name}}{{env_suffix}}_ADMIN;
        grant execute data metric function on account to role DCM_DEMO_2_{{team_name}}{{env_suffix}}_ADMIN;
    {% endif %}
{% endfor %}
