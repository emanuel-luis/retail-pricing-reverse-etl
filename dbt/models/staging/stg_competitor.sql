-- market.competitor, latest version per primary key (competitor_id).
select competitor_id,
       competitor_name,
       price_bias,
       version,
       updated_at
from {{ source('raw', 'market__competitor') }} final
