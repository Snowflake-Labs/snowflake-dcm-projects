-- loop through team dictionaries
{% for team in teams %}
    {% set team_name = team.name | upper %}

    -- inject dictionary values directly into object properties
    define schema DCM_DEMO_1{{env_suffix}}.{{team_name}}
        comment = 'using JINJA dictionary values'
        data_retention_time_in_days = {{ team.data_retention_days }};

    -- pass both the name and the dynamically resolved wh_size to your macro
    {{ create_team_roles(team_name) }}
        
    define table DCM_DEMO_1{{env_suffix}}.{{team_name}}.PRODUCTS(
        ITEM_NAME varchar,
        ITEM_ID varchar,
        ITEM_CATEGORY array
    )
    data_metric_schedule = 'TRIGGER_ON_CHANGES'
    ;

    attach data metric function SNOWFLAKE.CORE.NULL_COUNT
        to table DCM_DEMO_1{{env_suffix}}.{{team_name}}.PRODUCTS
        on (ITEM_ID)
        expectation NO_MISSING_ID (value = 0);
        
    {% if team_name == 'HR' %}
        define table DCM_DEMO_1{{env_suffix}}.{{team_name}}.EMPLOYEES(
            NAME varchar,
            ID int
        )
        comment = 'This table is only created in HR'
        ;
    {% endif %}
    
    -- use dictionary booleans to deploy optional infrastructure
    {% if team.needs_sandbox_schema | default(false) %}
        define schema DCM_DEMO_1{{env_suffix}}.{{team_name}}_SANDBOX
            comment = 'Sandbox schema defined via dictionary flag'
            data_retention_time_in_days = 1;
    {% endif %}

{% endfor %}


-- ### check the jinja_demo file in the PLAN output to see the rendered jinja 