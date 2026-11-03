-- sales.sale_return, latest version per primary key (return_id).
select return_id,
       sale_line_id,
       quantity,
       amount,
       business_date,
       version,
       updated_at
from {{ source('raw', 'sales__sale_return') }} final
