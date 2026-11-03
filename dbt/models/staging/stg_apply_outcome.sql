-- Apply status of every stage row, all four targets: one row per (cycle_id, target, key).
-- The stage tables are extracted like any OLTP table (version from the touch trigger), so FINAL
-- keeps the latest status of each row.
with zone_price as (
    select 'zone_price' as target, cycle_id, product_id, zone_id, toInt32(0) as store_id,
           toInt32(0) as promo_id, batch_no, new_price, status, status_detail, processed_at
    from {{ source('raw', 'pricing_apply__zone_price_stage') }} final
),
store_override as (
    select 'store_override', s.cycle_id, s.product_id, st.zone_id, s.store_id, toInt32(0),
           s.batch_no, s.new_price, s.status, s.status_detail, s.processed_at
    from {{ source('raw', 'pricing_apply__store_override_stage') }} as s final
    left join {{ ref('stg_store') }} st on st.store_id = s.store_id
),
promo_price as (
    select 'promo_price', cycle_id, product_id, zone_id, toInt32(0), promo_id, batch_no,
           new_price, status, status_detail, processed_at
    from {{ source('raw', 'pricing_apply__promo_price_stage') }} final
),
shelf_label as (
    select 'shelf_label', cycle_id, product_id, zone_id, store_id, toInt32(0), batch_no,
           price, status, status_detail, processed_at
    from {{ source('raw', 'pricing_apply__shelf_label_stage') }} final
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
