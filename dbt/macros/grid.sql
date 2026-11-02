{#
  Price-ending grid helpers (DECISIONS 38). `g` is the sorted Array(Int64) of grid prices in
  cents from int_price_grid; values are Int64 cents. A result of 0 means "no grid price".
  Lambda variable names are unique so the helpers can nest inside other lambdas.
#}
{% macro grid_floor(v, g='grid') -%}arrayLast(_gf -> _gf <= ({{ v }}), {{ g }}){%- endmacro %}
{% macro grid_ceil(v, g='grid') -%}arrayFirst(_gc -> _gc >= ({{ v }}), {{ g }}){%- endmacro %}
{% macro grid_next_above(v, g='grid') -%}arrayFirst(_ga -> _ga > ({{ v }}), {{ g }}){%- endmacro %}

{# Nearest grid price, a tie goes up. #}
{% macro grid_round(v, g='grid') -%}
multiIf({{ grid_floor(v, g) }} = 0, {{ grid_ceil(v, g) }},
        {{ grid_ceil(v, g) }} = 0, {{ grid_floor(v, g) }},
        {{ grid_ceil(v, g) }} - ({{ v }}) <= ({{ v }}) - {{ grid_floor(v, g) }}, {{ grid_ceil(v, g) }},
        {{ grid_floor(v, g) }})
{%- endmacro %}

{#
  Nearest grid price to t (cents, already inside [lo, hi]) that lies inside [lo, hi]; 0 if none.
  The nearest grid price is floor(t) or ceil(t); when it falls outside, the other one is the only
  candidate.
#}
{% macro grid_nearest_in(t, lo, hi, g='grid') -%}
multiIf({{ grid_round(t, g) }} between ({{ lo }}) and ({{ hi }}) and {{ grid_round(t, g) }} > 0,
            {{ grid_round(t, g) }},
        {{ grid_floor(t, g) }} between ({{ lo }}) and ({{ hi }}) and {{ grid_floor(t, g) }} > 0,
            {{ grid_floor(t, g) }},
        {{ grid_ceil(t, g) }} between ({{ lo }}) and ({{ hi }}) and {{ grid_ceil(t, g) }} > 0,
            {{ grid_ceil(t, g) }},
        toInt64(0))
{%- endmacro %}

{# Decimal value to Int64 cents, half up (ClickHouse rounds Decimal half away from zero). #}
{% macro cents_round(x) -%}toInt64(round(toDecimal128({{ x }}, 6) * 100)){%- endmacro %}

{# Integer basis points (1e-4) of a Decimal(5,4) rate such as max_change_pct. #}
{% macro e4(x) -%}toInt64(({{ x }}) * 10000){%- endmacro %}

{# Int64 cents to the OLTP price type numeric(10,2); exact (cents / 100 has two decimals). #}
{% macro cents_to_price(c) -%}CAST(toDecimal64({{ c }}, 2) / 100 AS Decimal(10, 2)){%- endmacro %}
