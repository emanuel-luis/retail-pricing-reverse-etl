{# Run variables. cycle_id and as_of are required at execution; 0 is a cycle only with ci: true. #}
{% macro cycle_id() -%}
    {%- set c = var('cycle_id', none) -%}
    {%- if execute and c is none -%}
        {{ exceptions.raise_compiler_error("var cycle_id is required") }}
    {%- endif -%}
    {%- if execute and (c | int) == 0 and not var('ci', false) -%}
        {{ exceptions.raise_compiler_error("cycle_id 0 is only allowed with --vars ci: true") }}
    {%- endif -%}
    toUInt64({{ (c or 0) | int }})
{%- endmacro %}

{% macro as_of() -%}
    {%- set d = var('as_of', none) -%}
    {%- if execute and d is none -%}
        {{ exceptions.raise_compiler_error("var as_of is required (the cycle's business date)") }}
    {%- endif -%}
    toDate32('{{ d or "1970-01-01" }}')
{%- endmacro %}

{# Pre-hook of cycle-scoped models: a rerun of a cycle replaces its rows (lightweight delete). #}
{% macro cycle_delete() -%}
    {%- if is_incremental() -%}
        DELETE FROM {{ this }} WHERE cycle_id = {{ cycle_id() }}
    {%- endif -%}
{%- endmacro %}

{# Exact decimal constant for money math (a bare 0.02 literal is Float64 in ClickHouse). #}
{% macro dec(x) -%}toDecimal128('{{ x }}', 10){%- endmacro %}
