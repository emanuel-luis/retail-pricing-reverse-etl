-- g02: every zone price that is not held keeps the category's minimum margin,
-- new * (1 - min_margin) >= cost (exact, in cents and basis points), unless the floor was above
-- the cap (FLOOR_LIMITED_BY_CAP) and the price is the highest grid price <= hi.
select product_id, zone_id, new_price, unit_cost, min_margin, primary_reason
from (
    select *, {{ grid_value() }} as grid
    from {{ ref('pricing_zone_price') }}
    where cycle_id = {{ cycle_id() }}
)
where coalesce(primary_reason, '') not like 'HOLD_%'
  and toInt64(new_price * 100) * (10000 - {{ e4('min_margin') }})
      < toInt64(unit_cost * 10000) * 100
  and not (floor_limited and toInt64(new_price * 100) = {{ grid_floor('toInt64(hi * 100)') }})
