{# The custom schema (stg, int, pricing, reverse, marts) is the ClickHouse database, verbatim. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ (custom_schema_name or target.schema) | trim }}
{%- endmacro %}
