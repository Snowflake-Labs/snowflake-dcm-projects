{% macro cents_to_currency(column_name) -%}
    ({{ column_name }} / 100)::number(12, 2)
{%- endmacro %}
