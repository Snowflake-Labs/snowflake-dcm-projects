{% macro incremental_watermark(column_name) -%}
    {#- Null-safe watermark: an empty target table loads every row.
        reprocess_days widens the window, e.g. to pick up late-arriving corrections.
        The task graph passes it in with --vars. -#}
    {%- if is_incremental() %}
    where {{ column_name }} > (
        select dateadd('day', -{{ var('reprocess_days', 0) | int }},
            coalesce(max({{ column_name }}), '1900-01-01'::timestamp_ntz))
        from {{ this }}
    )
    {%- endif %}
{%- endmacro %}
