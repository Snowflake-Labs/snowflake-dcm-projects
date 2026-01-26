-- ### jinja macro to create standard set of roles for each database
{% macro create_team_roles(team) %}
    
    define role {{ team }}_OWNER;
    define role {{ team }}_DEVELOPER; -- Consolidated role
    define role {{ team }}_USAGE;

    grant USAGE on database DCM_PROJECT_{{env_suffix}} to role {{ team }}_USAGE;
    grant USAGE on schema DCM_PROJECT_{{env_suffix}}.{{ team | upper }} to role {{ team }}_USAGE;
    grant OWNERSHIP on schema DCM_PROJECT_{{env_suffix}}.{{ team | upper }} to role {{ team }}_OWNER;

    grant CREATE DYNAMIC TABLE, CREATE TABLE, CREATE VIEW on schema DCM_PROJECT_{{env_suffix}}.{{ team | upper }} to role {{ team }}_DEVELOPER;
    
    grant role {{ team }}_USAGE to role {{ team }}_DEVELOPER;
    grant role {{ team }}_DEVELOPER to role {{ team }}_OWNER;
    grant role {{ team }}_OWNER to role {{ project_owner_role }};
    -- ensure that the DCM still holds all roles it transfers ownership to to avoid lock-out 
    
{% endmacro %}


-- loop through lists
{% for team in teams %}
    
    -- add functions/ "filters"
    define schema DCM_PROJECT_{{env_suffix}}.{{ team | upper }}
        comment = 'using JINJA FILTER for upper';

    -- Run the macro to create all roles and grants for this schema
    {{ create_team_roles(team) }}
        
    define table DCM_PROJECT_{{env_suffix}}.{{ team | upper }}.PRODUCTS(
        ITEM_NAME varchar,
        ITEM_ID varchar,
        ITEM_CATEGORY array
    )
    data_metric_schedule = 'TRIGGER_ON_CHANGES'
    ;

    attach data metric function SNOWFLAKE.CORE.NULL_COUNT
        to table DCM_PROJECT_{{env_suffix}}.{{ team | upper }}.PRODUCTS
        on (ITEM_ID)
        expectation NO_MISSING_ID (value = 0);
        
    -- define conditions 
    {% if team == 'HR' %}
        define table DCM_PROJECT_{{env_suffix}}.{{ team | upper }}.EMPLOYEES(
            NAME varchar,
            ID int
        )
        comment = 'This table is only created in HR'
        ;
    {% endif %}

{% endfor %}


-- ### check the jinja_demo file in the PLAN output to see the rendered jinja 