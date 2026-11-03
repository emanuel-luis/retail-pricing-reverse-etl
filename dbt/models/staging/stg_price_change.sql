-- pricing.price_change, latest version per primary key (change_id).
select change_id,
       cycle_id,
       target,
       product_id,
       zone_id,
       store_id,
       promo_id,
       old_price,
       new_price,
       reason_code,
       applied_at,
       version,
       updated_at
from {{ source('raw', 'pricing__price_change') }} final
