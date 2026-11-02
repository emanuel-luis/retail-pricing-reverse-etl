{{ config(order_by='(cycle_id, batch_no, store_id, product_id)') }}
-- cycle x store x product: every label of the cycle (labels are new rows, nothing to diff), in
-- the shape of pricing_apply.shelf_label_stage, batched by label_batch_size.
select cycle_id,
       store_id,
       product_id,
       {{ batch_no('store_id, product_id', var('label_batch_size')) }} as batch_no,
       price,
       promo_price,
       due_date,
       source_target,
       zone_id,
       {{ row_hash('store_id, product_id, price, promo_price, due_date') }} as row_hash
from {{ ref('pricing_shelf_label') }}
where cycle_id = {{ cycle_id() }}
