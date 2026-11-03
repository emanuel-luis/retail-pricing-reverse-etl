{#
  Price-ending grid helpers (DECISIONS 38), in closed form per band. `g` is the one-row tuple
  (band_ending, band_first, band_top) of int_price_grid (grid_value()): per band the ending and
  its first and last grid price, all Int64 cents. A band holds every n * 100 + ending between
  first and top. Values are Int64 cents; a result of 0 means "no grid price". Only arithmetic over
  the few bands runs per row (scanning a full grid array per row materializes it in every block).
  `arrayMap(_v -> ..., [v])[1]` evaluates v once; lambda names are unique so the helpers nest.
#}
{% macro grid_value() -%}
(select (band_ending, band_first, band_top) from {{ ref('int_price_grid') }})
{%- endmacro %}

{# Largest grid price <= v. #}
{% macro grid_floor(v, g='grid') -%}
arrayMap(_vf -> arrayMax(arrayMap((_e, _f, _t) ->
        if(_vf >= _e and least(intDiv(_vf - _e, 100) * 100 + _e, _t) >= _f,
           least(intDiv(_vf - _e, 100) * 100 + _e, _t), toInt64(0)),
    {{ g }}.1, {{ g }}.2, {{ g }}.3)), [toInt64({{ v }})])[1]
{%- endmacro %}

{# Smallest grid price >= v (endings are below 1.00, so _vc - _e + 99 >= 0 for v >= 0). #}
{% macro grid_ceil(v, g='grid') -%}
arrayMap(_vc -> arrayMap(_m -> if(_m = 9223372036854775807, toInt64(0), _m), [arrayMin(arrayMap((_e, _f, _t) ->
        if(greatest(_f, intDiv(_vc - _e + 99, 100) * 100 + _e) <= _t,
           greatest(_f, intDiv(_vc - _e + 99, 100) * 100 + _e), toInt64(9223372036854775807)),
    {{ g }}.1, {{ g }}.2, {{ g }}.3))])[1], [toInt64({{ v }})])[1]
{%- endmacro %}

{# Smallest grid price > v. #}
{% macro grid_next_above(v, g='grid') -%}{{ grid_ceil('(' ~ v ~ ') + 1', g) }}{%- endmacro %}

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
