{{ config(order_by='(cycle_id, target, product_id, zone_id, store_id, promo_id)') }}
-- cycle x target x key, changed rows only. Key parts that do not apply to a target are 0.
with changes as (
    select 'zone_price' as target, product_id, zone_id, toInt32(0) as store_id,
           toInt32(0) as promo_id, old_price, new_price, primary_reason, reasons
    from {{ ref('pricing_zone_price') }} where cycle_id = {{ cycle_id() }} and changed
    union all
    select 'store_override', product_id, zone_id, store_id, toInt32(0), old_price, new_price,
           primary_reason, reasons
    from {{ ref('pricing_store_override') }} where cycle_id = {{ cycle_id() }} and changed
    union all
    select 'promo_price', product_id, zone_id, toInt32(0), promo_id, old_price, new_price,
           primary_reason, reasons
    from {{ ref('pricing_promo_price') }} where cycle_id = {{ cycle_id() }} and changed
)
select {{ cycle_id() }} as cycle_id,
       target,
       product_id,
       zone_id,
       store_id,
       promo_id,
       old_price,
       new_price,
       toDecimal64(toDecimal128(new_price, 6) / old_price - 1, 6) as change_pct,
       primary_reason,
       reasons,
       {{ as_of() }} as as_of
from changes
