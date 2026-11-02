-- inventory.stock_movement, latest version per primary key (movement_id).
select movement_id,
       store_id,
       product_id,
       movement_type,
       qty,
       business_date,
       version,
       updated_at
from {{ source('raw', 'inventory__stock_movement') }} final
