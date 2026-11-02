-- pricing.shelf_label_queue, latest version per primary key (label_id).
select label_id,
       cycle_id,
       store_id,
       product_id,
       price,
       promo_price,
       due_date,
       status,
       version,
       updated_at
from {{ source('raw', 'pricing__shelf_label_queue') }} final
