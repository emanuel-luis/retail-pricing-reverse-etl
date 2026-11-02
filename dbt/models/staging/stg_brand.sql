-- master.brand, latest version per primary key (brand_id).
select brand_id,
       brand_name,
       brand_tier,
       version,
       updated_at
from {{ source('raw', 'master__brand') }} final
