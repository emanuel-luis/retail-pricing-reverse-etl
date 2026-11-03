{{ config(order_by='(product_id, zone_id)') }}
-- product x zone, all money in Int64 cents with exact integer bounds:
--   holds first: promo with policy exclude (HOLD_PROMO), no cost (HOLD_NO_COST);
--   floor = ceil(cost / (1 - min_margin)), cap = [ceil(cur * (1 - cap)), floor(cur * (1 + cap))];
--   lo = greatest(floor, cap low); if floor > hi then lo = hi (FLOOR_LIMITED_BY_CAP) and the
--   price is the highest grid price <= hi;
--   otherwise the grid price nearest to the clamped candidate inside [lo, hi]; none: HOLD_INFEASIBLE.
with
cents as (
    select c.*,
           {{ grid_value() }} as grid,
           toInt64(c.current_price * 100) as cur_c,
           {{ e4('c.max_change_pct') }} as cap_e4,
           c.unit_cost is not null as has_cost,
           if(has_cost,
              intDiv(toInt64(assumeNotNull(c.unit_cost) * 10000) * 100
                     + (10000 - {{ e4('c.min_margin') }}) - 1,
                     10000 - {{ e4('c.min_margin') }}),
              toInt64(0)) as floor_c,
           intDiv(cur_c * (10000 - cap_e4) + 9999, 10000) as cap_lo_c,
           intDiv(cur_c * (10000 + cap_e4), 10000) as hi_c,
           {{ cents_round('c.candidate_price') }} as cand_c,
           multiIf(c.has_promo and c.promo_policy = 'exclude', 'HOLD_PROMO',
                   not has_cost, 'HOLD_NO_COST', '') as pre_hold
    from {{ ref('int_price_candidate') }} c
),
bounds as (
    select *,
           floor_c > hi_c as floor_limited,
           if(floor_limited, hi_c, greatest(floor_c, cap_lo_c)) as lo_c,
           least(greatest(cand_c, lo_c), hi_c) as target_c,
           multiIf(floor_limited, 'FLOOR_LIMITED_BY_CAP',
                   cand_c < lo_c and floor_c >= cap_lo_c, 'MARGIN_FLOOR',
                   cand_c < lo_c or cand_c > hi_c, 'CAPPED', '') as binding_reason
    from cents
),
ended as (
    select *,
           if(floor_limited,
              if({{ grid_floor('hi_c') }} >= cap_lo_c, {{ grid_floor('hi_c') }}, toInt64(0)),
              {{ grid_nearest_in('target_c', 'lo_c', 'hi_c') }}) as ended_c,
           if(pre_hold != '', pre_hold, if(ended_c = 0, 'HOLD_INFEASIBLE', '')) as hold_reason
    from bounds
)
select product_id,
       zone_id,
       zone_channel,
       class_id,
       category_id,
       pack_units,
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
       cap_lo_c,
       lo_c,
       hi_c,
       cand_c,
       target_c,
       ended_c,
       floor_limited,
       binding_reason,
       hold_reason,
       if(hold_reason != '', cur_c, ended_c) as new_c,
       arrayFilter(r -> r != '', arrayConcat(component_reasons, [
           binding_reason,
           if(binding_reason in ('MARGIN_FLOOR', 'FLOOR_LIMITED_BY_CAP')
              and coalesce(cost_change_90d_pct > 0, false), 'COST_PASS_THROUGH', ''),
           if(hold_reason = '' and ended_c != target_c, 'ENDING_ROUND', ''),
           hold_reason])) as reasons
from ended
