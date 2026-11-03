{{ config(order_by='(product_id, zone_id)') }}
-- product x zone: on hand (stores of the zone; the WEB zone uses warehouses), days of cover at
-- the 28-day rate and aging since the last receipt, with the category rule's flags.
with
store_oh as (
    select st.zone_id as zone_id, ss.product_id as product_id,
           sum(ss.on_hand) as on_hand,
           max(ss.last_received_date) as last_received_date
    from {{ ref('stg_store_stock') }} ss
    inner join {{ ref('stg_store') }} st on st.store_id = ss.store_id
    group by st.zone_id, ss.product_id
),
wh_oh as (
    select product_id, sum(on_hand) as on_hand
    from {{ ref('stg_warehouse_stock') }}
    group by product_id
),
base as (
    select c.product_id as product_id,
           c.zone_id as zone_id,
           toInt64(coalesce(if(c.zone_channel = 'WEB', w.on_hand, s.on_hand), 0)) as on_hand,
           coalesce(d.rate_28, 0) as rate_28,
           if(c.zone_channel = 'WEB', null,
              dateDiff('day', s.last_received_date, {{ as_of() }})) as aging_days,
           c.cover_min_days as cover_min_days,
           c.cover_max_days as cover_max_days,
           c.aging_max_days as aging_max_days
    from {{ ref('int_current_state') }} c
    left join store_oh s on s.product_id = c.product_id and s.zone_id = c.zone_id
    left join wh_oh w on w.product_id = c.product_id
    left join {{ ref('int_demand') }} d on d.product_id = c.product_id and d.zone_id = c.zone_id
)
select product_id,
       zone_id,
       on_hand,
       rate_28,
       on_hand / greatest(rate_28, 0.01) as days_of_cover,
       aging_days,
       coalesce(days_of_cover > cover_max_days, false) as is_overstock,
       coalesce(aging_days > aging_max_days, false) as is_aged,
       coalesce(rate_28 > 0 and days_of_cover < cover_min_days, false) as is_low_cover
from base
