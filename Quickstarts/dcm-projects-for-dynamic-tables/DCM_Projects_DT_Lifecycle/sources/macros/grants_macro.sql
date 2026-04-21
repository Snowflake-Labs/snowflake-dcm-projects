 -- ### Jinja macro to create standard set of roles for each database
{% macro create_team_roles(team) %}

    DEFINE ROLE {{team}}_OWNER{{env_suffix}};
    DEFINE ROLE {{team}}_DEVELOPER{{env_suffix}};
    DEFINE ROLE {{team}}_USAGE{{env_suffix}};

    GRANT USAGE     ON DATABASE DCM_DEMO_3{{env_suffix}}        TO ROLE {{team}}_USAGE{{env_suffix}};
    GRANT USAGE     ON SCHEMA DCM_DEMO_3{{env_suffix}}.{{team}} TO ROLE {{team}}_USAGE{{env_suffix}};
    GRANT OWNERSHIP ON SCHEMA DCM_DEMO_3{{env_suffix}}.{{team}} TO ROLE {{team}}_OWNER{{env_suffix}};

    GRANT CREATE DYNAMIC TABLE, CREATE TABLE, CREATE VIEW
    ON SCHEMA DCM_DEMO_3{{env_suffix}}.{{team}} TO ROLE {{team}}_DEVELOPER{{env_suffix}};

    GRANT ROLE {{team}}_USAGE{{env_suffix}}     TO ROLE {{team}}_DEVELOPER{{env_suffix}};
    GRANT ROLE {{team}}_DEVELOPER{{env_suffix}} TO ROLE {{team}}_OWNER{{env_suffix}};
    GRANT ROLE {{team}}_OWNER{{env_suffix}}     TO ROLE {{project_owner_role}};

{% endmacro %}
