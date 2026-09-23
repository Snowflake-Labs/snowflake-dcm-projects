-- ### Jinja macro — the standard pair of roles for every team schema
{% macro create_team_roles(team) %}
    DEFINE ROLE {{team}}_OWNER{{env_suffix}} COMMENT = 'Owns the {{team}} team schema';
    DEFINE ROLE {{team}}_USAGE{{env_suffix}} COMMENT = 'Read access to the {{team}} team schema';
    GRANT USAGE     on database DCM_DEMO_1{{env_suffix}}        to role {{team}}_USAGE{{env_suffix}};
    GRANT USAGE     on schema DCM_DEMO_1{{env_suffix}}.{{team}} to role {{team}}_USAGE{{env_suffix}};
    GRANT OWNERSHIP on schema DCM_DEMO_1{{env_suffix}}.{{team}} to role {{team}}_OWNER{{env_suffix}};
    GRANT ROLE {{team}}_USAGE{{env_suffix}} to role {{team}}_OWNER{{env_suffix}};
    -- ensure that the DCM still holds all roles it transfers ownership to to avoid lock-out
    GRANT ROLE {{team}}_OWNER{{env_suffix}} to role {{project_owner_role}};
{% endmacro %}
