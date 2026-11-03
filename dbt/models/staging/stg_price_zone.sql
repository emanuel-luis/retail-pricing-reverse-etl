-- master.price_zone, latest version per primary key (zone_id).
select zone_id,
       zone_code,
       channel,
       price_factor,
       version,
       updated_at
from {{ source('raw', 'master__price_zone') }} final
