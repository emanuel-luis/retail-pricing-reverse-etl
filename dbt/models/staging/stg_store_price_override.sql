-- pricing.store_price_override, latest version per primary key (override_id).
select override_id,
       store_id,
       product_id,
       price,
       valid_from,
       valid_to,
       last_cycle_id,
       updated_by,
       version,
       updated_at
from {{ source('raw', 'pricing__store_price_override') }} final
