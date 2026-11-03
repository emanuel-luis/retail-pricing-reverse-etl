{{ config(order_by='(cycle_id, store_id, product_id)') }}
-- cycle x store x product, one row per open override. When the zone price changes, the override
-- keeps its ratio to it: new = new_zone * old_override / old_zone (cents, half up), then the
-- nearest grid price inside the category cap around the old override; none: HOLD_INFEASIBLE.
with o as (
    select o.store_id as store_id,
           o.product_id as product_id,
           s.zone_id as zone_id,
           o.version as expected_version,
           o.price as old_price,
           toInt64(o.price * 100) as old_c,
           toInt64(zp.old_price * 100) as zone_old_c,
           toInt64(zp.new_price * 100) as zone_new_c,
           zp.changed as zone_changed,
           zp.primary_reason as zone_reason,
           zp.reasons as zone_reasons,
           {{ e4('zp.max_change_pct') }} as cap_e4,
           {{ grid_value() }} as grid
    from {{ ref('stg_store_price_override') }} o
    inner join {{ ref('stg_store') }} s on s.store_id = o.store_id
    inner join (
        select * from {{ ref('pricing_zone_price') }} where cycle_id = {{ cycle_id() }}
    ) zp on zp.product_id = o.product_id and zp.zone_id = s.zone_id
    where o.valid_to is null
),
b as (
    select *,
           intDiv(2 * zone_new_c * old_c + zone_old_c, 2 * zone_old_c) as ratio_c,
           intDiv(old_c * (10000 - cap_e4) + 9999, 10000) as lo_c,
           intDiv(old_c * (10000 + cap_e4), 10000) as hi_c,
           least(greatest(ratio_c, lo_c), hi_c) as target_c
    from o
),
e as (
    select *,
           if(zone_changed, {{ grid_nearest_in('target_c', 'lo_c', 'hi_c') }}, old_c) as ended_c
    from b
)
select {{ cycle_id() }} as cycle_id,
       store_id,
       product_id,
       zone_id,
       old_price,
       {{ cents_to_price('if(ended_c = 0, old_c, ended_c)') }} as new_price,
       ended_c != 0 and ended_c != old_c as changed,
       multiIf(not zone_changed, NULL,
               ended_c = 0, 'HOLD_INFEASIBLE',
               ended_c = old_c, NULL,
               ratio_c != target_c, 'CAPPED',
               zone_reason) as primary_reason,
       arrayFilter(r -> r != '', arrayConcat(zone_reasons, [
           if(zone_changed and ended_c = 0, 'HOLD_INFEASIBLE', ''),
           if(zone_changed and ended_c != 0 and ratio_c != target_c, 'CAPPED', '')]))
           as reasons,
       toUInt64(expected_version) as expected_version
from e
