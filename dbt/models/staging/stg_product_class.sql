-- master.product_class, latest version per primary key (class_id).
select class_id,
       category_id,
       class_name,
       version,
       updated_at
from {{ source('raw', 'master__product_class') }} final
