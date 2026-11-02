-- sales.sale_line, latest version per primary key (sale_line_id).
select sale_line_id,
       sale_id,
       line_no,
       product_id,
       quantity,
       unit_price,
       discount_amount,
       net_amount,
       unit_cost,
       promo_id,
       version,
       updated_at
from {{ source('raw', 'sales__sale_line') }} final
