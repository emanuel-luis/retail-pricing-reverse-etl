{{ config(order_by='(cycle_id, target, key_text)') }}
-- cycle x target x key: version conflicts found by apply_batch, the engine's proposal against
-- the price a person set, and who set it.
select cycle_id,
       target,
       key_text,
       expected_version,
       actual_version,
       proposed_price,
       actual_price as human_price,
       actual_updated_by as editor,
       detected_at
from {{ ref('stg_apply_conflict') }}
