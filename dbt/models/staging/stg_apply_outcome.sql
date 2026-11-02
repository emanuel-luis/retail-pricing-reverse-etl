-- Apply status of every stage row, all four targets: one row per (cycle_id, target, key).
-- Stage rows change status in place and carry no version, so the latest extract wins.
{% set common = {'cycle_id': 'Int64', 'batch_no': 'Int32', 'status': 'String',
                 'status_detail': 'String', 'processed_at': "DateTime64(3, 'UTC')",
                 '_loaded_at': "DateTime64(3, 'UTC')"} %}
with zone_price as (
    select 'zone_price' as target, cycle_id, product_id, zone_id, toInt32(0) as store_id,
           toInt32(0) as promo_id, batch_no, new_price, status, status_detail, processed_at,
           _loaded_at
    from {{ raw_or_empty('pricing_apply__zone_price_stage',
           dict(common, product_id='Int64', zone_id='Int16', new_price='Decimal(10, 2)')) }}
),
store_override as (
    select 'store_override', s.cycle_id, s.product_id, st.zone_id, s.store_id, toInt32(0),
           s.batch_no, s.new_price, s.status, s.status_detail, s.processed_at, s._loaded_at
    from {{ raw_or_empty('pricing_apply__store_override_stage',
           dict(common, product_id='Int64', store_id='Int32', new_price='Decimal(10, 2)')) }} s
    left join {{ ref('stg_store') }} st on st.store_id = s.store_id
),
promo_price as (
    select 'promo_price', cycle_id, product_id, zone_id, toInt32(0), promo_id, batch_no,
           new_price, status, status_detail, processed_at, _loaded_at
    from {{ raw_or_empty('pricing_apply__promo_price_stage',
           dict(common, product_id='Int64', zone_id='Int16', promo_id='Int32',
                new_price='Decimal(10, 2)')) }}
),
shelf_label as (
    select 'shelf_label', cycle_id, product_id, zone_id, store_id, toInt32(0), batch_no,
           price, status, status_detail, processed_at, _loaded_at
    from {{ raw_or_empty('pricing_apply__shelf_label_stage',
           dict(common, product_id='Int64', zone_id='Int16', store_id='Int32',
                price='Decimal(10, 2)')) }}
),
all_rows as (
    select * from zone_price
    union all select * from store_override
    union all select * from promo_price
    union all select * from shelf_label
)
select target,
       toUInt64(cycle_id) as cycle_id,
       toInt64(product_id) as product_id,
       toInt16(coalesce(zone_id, 0)) as zone_id,
       toInt32(store_id) as store_id,
       toInt32(promo_id) as promo_id,
       batch_no,
       new_price,
       status,
       status_detail,
       processed_at
from all_rows
order by cycle_id, target, product_id, zone_id, store_id, promo_id, _loaded_at desc
limit 1 by cycle_id, target, product_id, zone_id, store_id, promo_id
