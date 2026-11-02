-- pricing.promo_item, latest version per primary key (promo_id, product_id).
select promo_id,
       product_id,
       version,
       updated_at
from {{ source('raw', 'pricing__promo_item') }} final
