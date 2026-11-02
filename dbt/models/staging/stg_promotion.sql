-- pricing.promotion, latest version per primary key (promo_id).
select promo_id,
       promo_code,
       start_date,
       end_date,
       discount_pct,
       channel,
       version,
       updated_at
from {{ source('raw', 'pricing__promotion') }} final
