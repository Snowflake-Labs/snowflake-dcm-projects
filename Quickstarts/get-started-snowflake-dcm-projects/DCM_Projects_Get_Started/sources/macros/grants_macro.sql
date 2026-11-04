-- ### Jinja macro: the role that owns each team schema
{% macro create_team_roles(team) %}
    DEFINE ROLE {{team}}_OWNER{{env_suffix}} COMMENT = 'Owns the {{team}} team schema';
    GRANT USAGE     ON DATABASE DCM_DEMO_1{{env_suffix}}        TO ROLE {{team}}_OWNER{{env_suffix}};
    GRANT OWNERSHIP ON SCHEMA DCM_DEMO_1{{env_suffix}}.{{team}} TO ROLE {{team}}_OWNER{{env_suffix}};
    -- ensure the project owner still holds every role it transfers ownership to,
    -- to avoid locking itself out
    GRANT ROLE {{team}}_OWNER{{env_suffix}} TO ROLE {{project_owner_role}};
{% endmacro %}
