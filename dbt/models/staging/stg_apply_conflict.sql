-- Version conflicts recorded by apply_batch: one row per (cycle_id, target, key_text).
select toUInt64(cycle_id) as cycle_id,
       target,
       key_text,
       expected_version,
       actual_version,
       proposed_price,
       actual_price,
       actual_updated_by,
       detected_at
from {{ source('raw', 'pricing_apply__conflict') }} final
