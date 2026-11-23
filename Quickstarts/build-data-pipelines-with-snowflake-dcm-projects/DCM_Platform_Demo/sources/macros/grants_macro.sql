{% macro create_team_role(team) %}

    -- One role per team. It owns the team's DCM projects and everything they
    -- deploy, so it gets what a project owner needs inside the team database.
    DEFINE ROLE DCM_DEMO_2_{{team}}{{env_suffix}}_ADMIN
        COMMENT = 'Owner role for the {{team}} team and its DCM projects';

    GRANT USAGE ON WAREHOUSE DCM_DEMO_2_{{team}}_WH{{env_suffix}} TO ROLE DCM_DEMO_2_{{team}}{{env_suffix}}_ADMIN;
    GRANT USAGE ON DATABASE DCM_DEMO_2_{{team}}{{env_suffix}} TO ROLE DCM_DEMO_2_{{team}}{{env_suffix}}_ADMIN;
    GRANT CREATE SCHEMA ON DATABASE DCM_DEMO_2_{{team}}{{env_suffix}} TO ROLE DCM_DEMO_2_{{team}}{{env_suffix}}_ADMIN;
    GRANT USAGE ON SCHEMA DCM_DEMO_2_{{team}}{{env_suffix}}.PROJECTS TO ROLE DCM_DEMO_2_{{team}}{{env_suffix}}_ADMIN;
    GRANT CREATE DCM PROJECT ON SCHEMA DCM_DEMO_2_{{team}}{{env_suffix}}.PROJECTS TO ROLE DCM_DEMO_2_{{team}}{{env_suffix}}_ADMIN;

    -- Roll the team role up to the platform project's owner, so whoever runs the
    -- platform can also act as any team.
    GRANT ROLE DCM_DEMO_2_{{team}}{{env_suffix}}_ADMIN TO ROLE {{project_owner_role}};

{% endmacro %}
