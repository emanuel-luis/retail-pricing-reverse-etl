-- g05: every changed price (zone, override, promo) is a price-ending grid price.
with changed as (
    select 'zone_price' as target, product_id, new_price
    from {{ ref('pricing_zone_price') }} where cycle_id = {{ cycle_id() }} and changed
    union all
    select 'store_override', product_id, new_price
    from {{ ref('pricing_store_override') }} where cycle_id = {{ cycle_id() }} and changed
    union all
    select 'promo_price', product_id, new_price
    from {{ ref('pricing_promo_price') }} where cycle_id = {{ cycle_id() }} and changed
)
select target, product_id, new_price
from changed
where {{ grid_floor('toInt64(new_price * 100)', grid_value()) }} != toInt64(new_price * 100)
