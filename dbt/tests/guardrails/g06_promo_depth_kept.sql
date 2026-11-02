-- g06: a recalculated promo price is the highest grid price at or below
-- new_regular * (1 - depth): never above the depth target, and within one grid step of it.
with p as (
    select promo_id, product_id, zone_id, toInt64(new_price * 100) as new_c,
           intDiv(toInt64(regular_new_price * 100) * (10000 - {{ e4('discount_pct') }}), 10000)
               as target_c,
           (select grid from {{ ref('int_price_grid') }}) as grid
    from {{ ref('pricing_promo_price') }}
    where cycle_id = {{ cycle_id() }}
)
select promo_id, product_id, zone_id, new_c, target_c
from p
where new_c > target_c or {{ grid_next_above('new_c') }} <= target_c
