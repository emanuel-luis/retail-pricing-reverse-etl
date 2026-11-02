{{ config(order_by='(cycle_id, product_id, zone_id)') }}
-- cycle x product x zone: 28-day units projected at the new price with the category elasticity,
-- units_new = units_28d * (new / old) ^ elasticity, and revenue and gross margin old vs new.
-- A projection, not a price: Float64 rounded to 4 decimals.
with p as (
    select zp.product_id as product_id,
           zp.zone_id as zone_id,
           toFloat64(zp.old_price) as old_p,
           toFloat64(zp.new_price) as new_p,
           toFloat64(coalesce(zp.unit_cost, 0)) as cost,
           toFloat64(coalesce(d.units_28d, 0)) as units_old,
           units_old * pow(new_p / old_p, toFloat64(zp.elasticity)) as units_new
    from (
        select * from {{ ref('pricing_zone_price') }} where cycle_id = {{ cycle_id() }}
    ) zp
    left join {{ ref('int_demand') }} d on d.product_id = zp.product_id and d.zone_id = zp.zone_id
)
select {{ cycle_id() }} as cycle_id,
       product_id,
       zone_id,
       round(units_old, 4) as units_28d,
       round(units_new, 4) as units_28d_new,
       round(units_old * old_p, 4) as revenue_old,
       round(units_new * new_p, 4) as revenue_new,
       round(units_old * (old_p - cost), 4) as margin_old,
       round(units_new * (new_p - cost), 4) as margin_new,
       round(units_new * new_p - units_old * old_p, 4) as revenue_delta,
       round(units_new * (new_p - cost) - units_old * (old_p - cost), 4) as margin_delta
from p
