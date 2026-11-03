-- rules.kvi, latest version per primary key (product_id).
select product_id,
       kvi_tier,
       competitor_index_target,
       version,
       updated_at
from {{ source('raw', 'rules__kvi') }} final
