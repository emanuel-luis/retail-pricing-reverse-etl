-- master.store, latest version per primary key (store_id).
select store_id,
       store_code,
       store_name,
       zone_id,
       city,
       state,
       price_adjust_pct,
       version,
       updated_at
from {{ source('raw', 'master__store') }} final
