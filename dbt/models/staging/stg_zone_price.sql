-- pricing.zone_price, latest version per primary key (product_id, zone_id).
select product_id,
       zone_id,
       price,
       effective_from,
       last_cycle_id,
       updated_by,
       version,
       updated_at
from {{ source('raw', 'pricing__zone_price') }} final
