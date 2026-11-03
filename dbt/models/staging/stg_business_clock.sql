-- ops.business_clock, latest version per primary key (id).
select id,
       business_date,
       version,
       updated_at
from {{ source('raw', 'ops__business_clock') }} final
