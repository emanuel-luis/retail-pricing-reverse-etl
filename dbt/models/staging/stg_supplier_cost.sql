-- pricing.supplier_cost, latest version per primary key (product_id, valid_from).
select product_id,
       valid_from,
       supplier_id,
       unit_cost,
       version,
       updated_at
from {{ source('raw', 'pricing__supplier_cost') }} final
