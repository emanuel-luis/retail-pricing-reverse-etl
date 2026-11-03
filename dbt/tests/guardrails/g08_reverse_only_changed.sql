-- g08: a reverse row is sent only when its target row hash differs from the extracted snapshot.
select 'zone_price' as target, r.product_id as product_id, toInt64(r.zone_id) as k2
from {{ ref('rev_zone_price') }} r
inner join {{ ref('stg_zone_price') }} s on s.product_id = r.product_id and s.zone_id = r.zone_id
where r.cycle_id = {{ cycle_id() }}
  and cityHash64(r.product_id, r.zone_id, r.new_price)
      = cityHash64(s.product_id, s.zone_id, s.price)
union all
select 'store_override', r.product_id, toInt64(r.store_id)
from {{ ref('rev_store_override') }} r
inner join (
    select * from {{ ref('stg_store_price_override') }} where valid_to is null
) s on s.store_id = r.store_id and s.product_id = r.product_id
where r.cycle_id = {{ cycle_id() }}
  and cityHash64(r.store_id, r.product_id, r.new_price)
      = cityHash64(s.store_id, s.product_id, s.price)
union all
select 'promo_price', r.product_id, toInt64(r.promo_id)
from {{ ref('rev_promo_price') }} r
inner join {{ ref('stg_promo_price') }} s
    on s.promo_id = r.promo_id and s.product_id = r.product_id and s.zone_id = r.zone_id
where r.cycle_id = {{ cycle_id() }}
  and cityHash64(r.promo_id, r.product_id, r.zone_id, r.new_price)
      = cityHash64(s.promo_id, s.product_id, s.zone_id, s.promo_price)
