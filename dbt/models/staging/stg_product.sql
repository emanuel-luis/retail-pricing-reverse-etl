-- master.product, latest version per primary key (product_id).
select product_id,
       product_code,
       product_name,
       class_id,
       brand_id,
       supplier_id,
       size,
       pack_units,
       is_active,
       version,
       updated_at
from {{ source('raw', 'master__product') }} final
