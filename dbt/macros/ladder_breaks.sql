{#
  Ladder violations (guardrails g03/g04) in groups where the engine changed at least one member
  this cycle. A group the engine left untouched keeps its current prices whatever the reason
  (every member held, or nothing to do); manual edits may have broken it, which is not the
  engine's doing and must not stop the cycle.

  prices:  relation (product_id, zone_id, new_price, changed)
  members: relation (ladder_group_id, ladder_type, product_id, rank, pack_units)
  PACK: unit price (price / pack) must not rise with rank. TIER: price must rise with rank.
#}
{% macro ladder_breaks(kind, prices, members) -%}
with lm as (
    select m.ladder_group_id as ladder_group_id, m.rank as rank, p.zone_id as zone_id,
           toInt64(p.new_price * 100) as p, toInt64(m.pack_units * 100) as pack_h,
           p.changed as changed
    from {{ members }} m
    inner join {{ prices }} p on p.product_id = m.product_id
    where m.ladder_type = '{{ kind }}'
),
touched as (
    select ladder_group_id, zone_id from lm group by ladder_group_id, zone_id having max(changed)
)
select a.ladder_group_id as ladder_group_id, a.zone_id as zone_id,
       a.rank as rank_a, a.p as price_a, b.rank as rank_b, b.p as price_b
from lm a
inner join lm b on b.ladder_group_id = a.ladder_group_id and b.zone_id = a.zone_id
where a.rank < b.rank
  and {% if kind == 'PACK' %}a.p * b.pack_h < b.p * a.pack_h{% else %}a.p >= b.p{% endif %}
  and (a.ladder_group_id, a.zone_id) in (select ladder_group_id, zone_id from touched)
{%- endmacro %}

{# The cycle's zone prices and the ladder members, as ladder_breaks reads them. #}
{% macro ladder_breaks_inputs() -%}
prices as (
    select product_id, zone_id, new_price, changed
    from {{ ref('pricing_zone_price') }} where cycle_id = {{ cycle_id() }}
),
members as (
    select lm.ladder_group_id as ladder_group_id, g.ladder_type as ladder_type,
           lm.product_id as product_id, lm.rank as rank, p.pack_units as pack_units
    from {{ ref('stg_ladder_member') }} lm
    inner join {{ ref('stg_ladder_group') }} g on g.ladder_group_id = lm.ladder_group_id
    inner join {{ ref('stg_product') }} p on p.product_id = lm.product_id
)
{%- endmacro %}
