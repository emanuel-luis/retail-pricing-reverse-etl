-- rules.price_ending_band, latest version per primary key (band_id).
select band_id,
       min_price,
       max_price,
       ending,
       version,
       updated_at
from {{ source('raw', 'rules__price_ending_band') }} final
