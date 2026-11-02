-- inventory.store_stock, latest version per primary key (store_id, product_id).
select store_id,
       product_id,
       on_hand,
       last_received_date,
       version,
       updated_at
from {{ source('raw', 'inventory__store_stock') }} final
