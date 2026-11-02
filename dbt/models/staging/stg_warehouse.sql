-- master.warehouse, latest version per primary key (warehouse_id).
select warehouse_id,
       warehouse_code,
       warehouse_name,
       city,
       state,
       version,
       updated_at
from {{ source('raw', 'master__warehouse') }} final
