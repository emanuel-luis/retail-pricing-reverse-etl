-- inventory.warehouse_stock, latest version per primary key (warehouse_id, product_id).
select warehouse_id,
       product_id,
       on_hand,
       version,
       updated_at
from {{ source('raw', 'inventory__warehouse_stock') }} final
