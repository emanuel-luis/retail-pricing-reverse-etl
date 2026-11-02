-- pricing.promo_price, latest version per primary key (promo_id, product_id, zone_id).
select promo_id,
       product_id,
       zone_id,
       promo_price,
       last_cycle_id,
       updated_by,
       version,
       updated_at
from {{ source('raw', 'pricing__promo_price') }} final
