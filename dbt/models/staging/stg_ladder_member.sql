-- rules.ladder_member, latest version per primary key (ladder_group_id, product_id).
select ladder_group_id,
       product_id,
       rank,
       version,
       updated_at
from {{ source('raw', 'rules__ladder_member') }} final
