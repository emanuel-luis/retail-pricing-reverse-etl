-- master.category, latest version per primary key (category_id).
select category_id,
       category_name,
       version,
       updated_at
from {{ source('raw', 'master__category') }} final
