{{ config(order_by='(cycle_id, batch_no, promo_id, product_id, zone_id)') }}
-- cycle x promo x product x zone: changed promo prices, in the shape of
-- pricing_apply.promo_price_stage.
select cycle_id,
       promo_id,
       product_id,
       zone_id,
       {{ batch_no('promo_id, product_id, zone_id', var('batch_size')) }} as batch_no,
       new_price,
       old_price,
       expected_version,
       primary_reason as reason_code,
       {{ row_hash('promo_id, product_id, zone_id, new_price') }} as row_hash
from {{ ref('pricing_promo_price') }}
where cycle_id = {{ cycle_id() }}
  and cityHash64(promo_id, product_id, zone_id, new_price)
      != cityHash64(promo_id, product_id, zone_id, old_price)
