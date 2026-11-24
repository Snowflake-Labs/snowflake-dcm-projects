{% macro generate_schema_name(custom_schema_name, node) -%}
    {#- Use the schema name as configured, without the target-schema prefix,
        so that dbt writes into the schema that DCM manages. -#}
    {{ (custom_schema_name or target.schema) | trim | upper }}
{%- endmacro %}
