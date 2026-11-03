-- market.competitor_price, latest version per primary key (observation_id).
select observation_id,
       competitor_id,
       product_id,
       zone_id,
       observed_date,
       price,
       version,
       updated_at
from {{ source('raw', 'market__competitor_price') }} final
