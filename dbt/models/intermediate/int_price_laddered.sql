{{ config(order_by='(product_id, zone_id)') }}
-- product x zone: ladders after bounds and ending. Passes PACK, TIER, PACK, TIER (raise only,
-- each member inside its own hi; held rows are frozen at their current price). If any group of
-- a class cannot be satisfied in a zone (a member would leave its range, or a ladder still
-- breaks after the passes), every ladder member of that class holds in that zone
-- (HOLD_LADDER): PACK and TIER groups of a class share products, so holding only one group
-- could break the other. Changed members get LADDER_ADJUST.
{%- set passes = ['PACK', 'TIER', 'PACK', 'TIER'] %}
with
members as (
    select m.ladder_group_id as ladder_group_id,
           g.ladder_type as ladder_type,
           p.class_id as class_id,
           m.product_id as product_id,
           m.rank as rank,
           toInt64(p.pack_units * 100) as pack_h
    from {{ ref('stg_ladder_member') }} m
    inner join {{ ref('stg_ladder_group') }} g on g.ladder_group_id = m.ladder_group_id
    inner join {{ ref('stg_product') }} p on p.product_id = m.product_id
),
p0 as (
    select product_id, zone_id, new_c as p,
           if(hold_reason != '', cur_c, hi_c) as hi, toUInt8(0) as bad
    from {{ ref('int_price_bounded') }}
    where product_id in (select product_id from {{ ref('stg_ladder_member') }})
),
{%- for kind in passes %}
p{{ loop.index }} as (
    {{ ladder_pass(kind, 'p' ~ loop.index0, 'members', ref('int_price_grid')) }}
),
{%- endfor %}
final_prices as (
    select m.ladder_group_id as ladder_group_id, m.ladder_type as ladder_type,
           m.class_id as class_id, m.product_id as product_id, m.rank as rank,
           m.pack_h as pack_h, f.zone_id as zone_id, f.p as p, f.bad as bad
    from members m
    inner join p{{ passes | length }} f on f.product_id = m.product_id
),
broken_groups as (
    select a.ladder_group_id as ladder_group_id, a.zone_id as zone_id
    from final_prices a
    inner join final_prices b on b.ladder_group_id = a.ladder_group_id and b.zone_id = a.zone_id
    where a.rank < b.rank
      and ((a.ladder_type = 'PACK' and a.p * b.pack_h < b.p * a.pack_h)
           or (a.ladder_type = 'TIER' and a.p >= b.p))
),
held_classes as (
    select distinct class_id, zone_id from final_prices
    where bad = 1
       or (ladder_group_id, zone_id) in (select ladder_group_id, zone_id from broken_groups)
),
laddered as (
    select b.*,
           f.p as ladder_c,
           b.hold_reason = '' and f.p is not null
               and (b.class_id, b.zone_id) in (select class_id, zone_id from held_classes)
               as ladder_hold
    from {{ ref('int_price_bounded') }} b
    left join (select product_id, zone_id, p from final_prices
               limit 1 by product_id, zone_id) f
        on f.product_id = b.product_id and f.zone_id = b.zone_id
),
resolved as (
    select *,
           hold_reason = '' and not ladder_hold and ladder_c is not null and ladder_c != new_c
               as ladder_adjusted,
           if(ladder_hold, 'HOLD_LADDER', hold_reason) as final_hold,
           multiIf(hold_reason != '' or ladder_hold, cur_c,
                   ladder_c is not null, assumeNotNull(ladder_c),
                   new_c) as final_c,
           arrayConcat(reasons,
                       if(ladder_hold, ['HOLD_LADDER'], []),
                       if(ladder_adjusted, ['LADDER_ADJUST'], [])) as final_reasons
    from laddered
)
select product_id,
       zone_id,
       zone_channel,
       class_id,
       category_id,
       current_price,
       current_version,
       promo_policy,
       min_margin,
       max_change_pct,
       unit_cost,
       elasticity,
       candidate_price,
       top_component,
       cur_c,
       floor_c,
       lo_c,
       hi_c,
       cand_c,
       floor_limited,
       binding_reason,
       ladder_adjusted,
       final_hold as hold_reason,
       final_c as new_c,
       final_reasons as reasons
from resolved
