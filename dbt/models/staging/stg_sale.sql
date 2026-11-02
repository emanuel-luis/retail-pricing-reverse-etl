-- sales.sale, latest version per primary key (sale_id).
select sale_id,
       channel,
       store_id,
       customer_id,
       business_date,
       sold_at,
       total_amount,
       version,
       updated_at
from {{ source('raw', 'sales__sale') }} final
