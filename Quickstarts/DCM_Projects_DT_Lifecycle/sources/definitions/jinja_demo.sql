-- Loop through team dictionaries
{% for team in teams %}
    {% set team_name = team.name | upper %}

    -- Inject dictionary values directly into object properties
    DEFINE SCHEMA DCM_DEMO_1{{env_suffix}}.{{team_name}}
        COMMENT = 'using JINJA dictionary values'
        DATA_RETENTION_TIME_IN_DAYS = {{ team.data_retention_days }};

    -- Pass both the name and the dynamically resolved wh_size to your macro
    {{ create_team_roles(team_name) }}

    DEFINE TABLE DCM_DEMO_1{{env_suffix}}.{{team_name}}.PRODUCTS(
        ITEM_NAME VARCHAR,
        ITEM_ID VARCHAR,
        ITEM_CATEGORY ARRAY
    )
    DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

    ATTACH DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT
        TO TABLE DCM_DEMO_1{{env_suffix}}.{{team_name}}.PRODUCTS
        ON (ITEM_ID)
        EXPECTATION NO_MISSING_ID (value = 0);

    {% if team_name == 'HR' %}
        DEFINE TABLE DCM_DEMO_1{{env_suffix}}.{{team_name}}.EMPLOYEES(
            NAME VARCHAR,
            ID INT
        )
        COMMENT = 'This table is only created in HR';
    {% endif %}

    -- Use dictionary booleans to deploy optional infrastructure
    {% if team.needs_sandbox_schema | default(false) %}
        DEFINE SCHEMA DCM_DEMO_1{{env_suffix}}.{{team_name}}_SANDBOX
            COMMENT = 'Sandbox schema defined via dictionary flag'
            DATA_RETENTION_TIME_IN_DAYS = 1;
    {% endif %}
{% endfor %}

-- ### Check the jinja_demo file in the PLAN output to see the rendered jinja
