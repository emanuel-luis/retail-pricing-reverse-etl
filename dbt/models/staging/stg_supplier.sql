-- master.supplier, latest version per primary key (supplier_id).
select supplier_id,
       supplier_name,
       version,
       updated_at
from {{ source('raw', 'master__supplier') }} final
