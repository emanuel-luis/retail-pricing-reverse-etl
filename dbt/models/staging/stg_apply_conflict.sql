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
from {{ raw_or_empty('pricing_apply__conflict', {
        'cycle_id': 'Int64', 'target': 'String', 'key_text': 'String',
        'expected_version': 'Int64', 'actual_version': 'Int64',
        'proposed_price': 'Decimal(10, 2)', 'actual_price': 'Decimal(10, 2)',
        'actual_updated_by': 'String', 'detected_at': "DateTime64(3, 'UTC')",
        '_loaded_at': "DateTime64(3, 'UTC')"}) }}
order by cycle_id, target, key_text, _loaded_at desc
limit 1 by cycle_id, target, key_text
