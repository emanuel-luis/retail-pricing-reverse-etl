-- pricing.price_history, latest version per primary key (history_id).
select history_id,
       product_id,
       zone_id,
       price,
       valid_from,
       valid_to,
       source,
       cycle_id,
       version,
       updated_at
from {{ source('raw', 'pricing__price_history') }} final
