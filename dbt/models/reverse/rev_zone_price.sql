{{ config(order_by='(cycle_id, batch_no, product_id, zone_id)') }}
-- cycle x product x zone: the rows of pricing_zone_price whose target row (key, price) hashes
-- differently from the extracted snapshot row, in the shape of pricing_apply.zone_price_stage.
select cycle_id,
       product_id,
       zone_id,
       {{ batch_no('product_id, zone_id', var('batch_size')) }} as batch_no,
       new_price,
       old_price,
       expected_version,
       primary_reason as reason_code,
       {{ row_hash('product_id, zone_id, new_price') }} as row_hash
from {{ ref('pricing_zone_price') }}
where cycle_id = {{ cycle_id() }}
  and cityHash64(product_id, zone_id, new_price) != cityHash64(product_id, zone_id, old_price)
