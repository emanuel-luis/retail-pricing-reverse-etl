{#
  The pricing_apply stage and conflict extracts may be absent from raw (CI loads only the
  contract tables). Returns the source, or a typed empty relation with the given columns.
#}
{% macro raw_or_empty(table, columns) -%}
    {%- set rel = source('raw', table) -%}
    {%- if execute and load_relation(rel) is none -%}
        (select {% for name, type in columns.items() -%}
            assumeNotNull(CAST(NULL AS Nullable({{ type }}))) AS {{ name }}{{ ', ' if not loop.last }}
        {%- endfor %} where 0)
    {%- else -%}
        {{ rel }}
    {%- endif -%}
{%- endmacro %}
