-- ### Per-team infrastructure
--
-- One loop over the teams in the manifest. Every team gets the same footprint:
-- its own warehouse, its own database with a PROJECTS schema to host the team's
-- own DCM projects, and one team role (from the macro in
-- sources/macros/grants_macro.sql). DEV has two teams, PROD four.
--
-- raw_access decides how much of the shared RAW schema the team can use.
-- The INHERITED grants cover every current and future table in the schema, so a
-- staging table added later is readable by the right teams without a new grant.
-- They need the account opt-in enabled in scripts/01_pre_deploy.sql.

{% for team in teams %}
    {% set team_name = team.name | upper %}

    DEFINE WAREHOUSE DCM_DEMO_2_{{team_name}}_WH{{env_suffix}}
    WITH
        WAREHOUSE_SIZE = '{{wh_size}}'
        AUTO_SUSPEND = 60
        COMMENT = 'Compute for the {{team.name}} team in the DCM Build Data Pipelines Quickstart';

    DEFINE DATABASE DCM_DEMO_2_{{team_name}}{{env_suffix}}
        COMMENT = 'Database owned by the {{team.name}} team';

    DEFINE SCHEMA DCM_DEMO_2_{{team_name}}{{env_suffix}}.PUBLIC
        COMMENT = 'Default schema, declared so DCM manages it';

    DEFINE SCHEMA DCM_DEMO_2_{{team_name}}{{env_suffix}}.PROJECTS
        COMMENT = 'Hosts the DCM projects of the {{team.name}} team';

    {{ create_team_role(team_name) }}

    {% if team.raw_access in ('READ', 'WRITE') %}
        GRANT USAGE ON DATABASE DCM_DEMO_2{{env_suffix}} TO ROLE DCM_DEMO_2_{{team_name}}{{env_suffix}}_ADMIN;
        GRANT USAGE ON SCHEMA DCM_DEMO_2{{env_suffix}}.RAW TO ROLE DCM_DEMO_2_{{team_name}}{{env_suffix}}_ADMIN;
        GRANT INHERITED SELECT ON ALL TABLES IN SCHEMA DCM_DEMO_2{{env_suffix}}.RAW TO ROLE DCM_DEMO_2_{{team_name}}{{env_suffix}}_ADMIN;
    {% endif %}
    {% if team.raw_access == 'WRITE' %}
        GRANT INHERITED INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA DCM_DEMO_2{{env_suffix}}.RAW TO ROLE DCM_DEMO_2_{{team_name}}{{env_suffix}}_ADMIN;
    {% endif %}

    -- The Finance pipeline attaches data quality expectations and reads their
    -- results. DCM_DEVELOPER can pass these on because 01_pre_deploy.sql granted
    -- it EXECUTE DATA METRIC FUNCTION WITH GRANT OPTION.
    {% if team_name == 'FINANCE' %}
        GRANT EXECUTE DATA METRIC FUNCTION ON ACCOUNT TO ROLE DCM_DEMO_2_{{team_name}}{{env_suffix}}_ADMIN;
        GRANT APPLICATION ROLE SNOWFLAKE.DATA_QUALITY_MONITORING_VIEWER TO ROLE DCM_DEMO_2_{{team_name}}{{env_suffix}}_ADMIN;
    {% endif %}
{% endfor %}
