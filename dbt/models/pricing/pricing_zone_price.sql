{{ config(order_by='(cycle_id, product_id, zone_id)') }}
-- cycle x product x zone: every zone price of the extract with its proposed new price.
select {{ cycle_id() }} as cycle_id,
       product_id,
       zone_id,
       class_id,
       category_id,
       current_price as old_price,
       {{ cents_to_price('new_c') }} as new_price,
       new_c != cur_c as changed,
       {{ primary_reason('hold_reason', 'reasons', 'top_component', 'new_c != cur_c') }}
           as primary_reason,
       reasons,
       {{ cents_to_price('lo_c') }} as lo,
       {{ cents_to_price('hi_c') }} as hi,
       toUInt64(current_version) as expected_version,
       elasticity,
       unit_cost,
       min_margin,
       max_change_pct,
       promo_policy,
       floor_limited
from {{ ref('int_price_laddered') }}
