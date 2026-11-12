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

    -- A read-only role for the team's consumers: analysts and dashboards that
    -- query what the team publishes but never change it. It is a database role,
    -- so it lives inside the team database and cannot be granted anything
    -- outside it. The INHERITED grant covers every schema and table the team's
    -- own projects create later, so the platform never needs to know their names.
    DEFINE DATABASE ROLE DCM_DEMO_2_{{team}}{{env_suffix}}.READER
        COMMENT = 'Read-only access to everything the {{team}} team publishes';

    GRANT USAGE ON DATABASE DCM_DEMO_2_{{team}}{{env_suffix}} TO DATABASE ROLE DCM_DEMO_2_{{team}}{{env_suffix}}.READER;
    GRANT INHERITED USAGE ON ALL SCHEMAS IN DATABASE DCM_DEMO_2_{{team}}{{env_suffix}} TO DATABASE ROLE DCM_DEMO_2_{{team}}{{env_suffix}}.READER;
    GRANT INHERITED SELECT ON ALL TABLES IN DATABASE DCM_DEMO_2_{{team}}{{env_suffix}} TO DATABASE ROLE DCM_DEMO_2_{{team}}{{env_suffix}}.READER;
    GRANT INHERITED SELECT ON ALL DYNAMIC TABLES IN DATABASE DCM_DEMO_2_{{team}}{{env_suffix}} TO DATABASE ROLE DCM_DEMO_2_{{team}}{{env_suffix}}.READER;

    -- The team admin holds the reader role too, and hands it out to its consumers.
    GRANT DATABASE ROLE DCM_DEMO_2_{{team}}{{env_suffix}}.READER TO ROLE DCM_DEMO_2_{{team}}{{env_suffix}}_ADMIN;

{% endmacro %}
