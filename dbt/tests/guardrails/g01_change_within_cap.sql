-- g01: no changed price moves beyond the category cap. Zone and override prices are checked
-- against [ceil(old * (1 - cap)), floor(old * (1 + cap))] in cents. Promo prices follow the
-- depth rule instead (a grid floor can move them more than the cap; g06 checks them): here a
-- promo price must stay below its new regular price.
with zone as (
    select 'zone_price' as target, toInt64(product_id) as product_id, toInt64(zone_id) as k2,
           toInt64(old_price * 100) as old_c, toInt64(new_price * 100) as new_c,
           {{ e4('max_change_pct') }} as cap_e4
    from {{ ref('pricing_zone_price') }}
    where cycle_id = {{ cycle_id() }} and changed
),
ovr as (
    select 'store_override', toInt64(o.product_id), toInt64(o.store_id),
           toInt64(o.old_price * 100), toInt64(o.new_price * 100), {{ e4('z.max_change_pct') }}
    from {{ ref('pricing_store_override') }} o
    inner join {{ ref('pricing_zone_price') }} z
        on z.cycle_id = o.cycle_id and z.product_id = o.product_id and z.zone_id = o.zone_id
    where o.cycle_id = {{ cycle_id() }} and o.changed
),
capped as (select * from zone union all select * from ovr)
select target, product_id, k2, old_c, new_c, cap_e4
from capped
where new_c > intDiv(old_c * (10000 + cap_e4), 10000)
   or new_c < intDiv(old_c * (10000 - cap_e4) + 9999, 10000)
union all
select 'promo_price', toInt64(product_id), toInt64(promo_id), toInt64(old_price * 100),
       toInt64(new_price * 100), 0
from {{ ref('pricing_promo_price') }}
where cycle_id = {{ cycle_id() }} and new_price >= regular_new_price
