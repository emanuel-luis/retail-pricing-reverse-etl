-- rules.ladder_group, latest version per primary key (ladder_group_id).
select ladder_group_id,
       ladder_type,
       class_id,
       brand_id,
       size,
       version,
       updated_at
from {{ source('raw', 'rules__ladder_group') }} final
