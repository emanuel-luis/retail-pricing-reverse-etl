{{ config(order_by='(cycle_id, batch_no, store_id, product_id)') }}
-- cycle x store x product: changed open overrides, in the shape of
-- pricing_apply.store_override_stage (expected_version 0 would create one; the engine only
-- reprices existing overrides). zone_id is extra, for the reconcile grouping.
select cycle_id,
       store_id,
       product_id,
       {{ batch_no('store_id, product_id', var('batch_size')) }} as batch_no,
       new_price,
       old_price,
       expected_version,
       primary_reason as reason_code,
       {{ row_hash('store_id, product_id, new_price') }} as row_hash,
       zone_id
from {{ ref('pricing_store_override') }}
where cycle_id = {{ cycle_id() }}
  and cityHash64(store_id, product_id, new_price) != cityHash64(store_id, product_id, old_price)
