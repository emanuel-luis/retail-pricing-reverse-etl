-- rules.category_rule, latest version per primary key (category_id).
select category_id,
       target_margin,
       min_margin,
       max_change_pct,
       cover_min_days,
       cover_max_days,
       aging_max_days,
       promo_policy,
       version,
       updated_at
from {{ source('raw', 'rules__category_rule') }} final
