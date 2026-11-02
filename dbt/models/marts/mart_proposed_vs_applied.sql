{{ config(order_by='(cycle_id, target, product_id, zone_id, store_id, promo_id)') }}
-- cycle x target x key: every row sent to the OLTP, its apply outcome (applied, unchanged,
-- conflict, rejected, staged, or not_sent when no stage row was extracted), the value in the
-- OLTP now, and whether an applied value was overwritten later. Complete for cycle N once
-- cycle N + 1 has extracted the stage tables.
with
proposed as (
    select 'zone_price' as target, cycle_id, product_id, zone_id, toInt32(0) as store_id,
           toInt32(0) as promo_id, new_price as proposed_price
    from {{ ref('rev_zone_price') }}
    union all
    select 'store_override', cycle_id, product_id, zone_id, store_id, toInt32(0), new_price
    from {{ ref('rev_store_override') }}
    union all
    select 'promo_price', cycle_id, product_id, zone_id, toInt32(0), promo_id, new_price
    from {{ ref('rev_promo_price') }}
    union all
    select 'shelf_label', cycle_id, product_id, zone_id, store_id, toInt32(0), price
    from {{ ref('rev_shelf_label') }}
),
now_values as (
    select 'zone_price' as target, product_id, zone_id, toInt32(0) as store_id,
           toInt32(0) as promo_id, price as value_now
    from {{ ref('stg_zone_price') }}
    union all
    select 'store_override', o.product_id, s.zone_id, o.store_id, toInt32(0), o.price
    from {{ ref('stg_store_price_override') }} o
    inner join {{ ref('stg_store') }} s on s.store_id = o.store_id
    where o.valid_to is null
    union all
    select 'promo_price', product_id, zone_id, toInt32(0), promo_id, promo_price
    from {{ ref('stg_promo_price') }}
)
select p.cycle_id as cycle_id,
       p.target as target,
       p.product_id as product_id,
       toInt16(p.zone_id) as zone_id,
       p.store_id as store_id,
       p.promo_id as promo_id,
       p.proposed_price as proposed_price,
       coalesce(o.status, 'not_sent') as outcome,
       o.status_detail as status_detail,
       if(p.target = 'shelf_label', q.price, n.value_now) as value_now,
       outcome = 'applied' and value_now is not null and value_now != p.proposed_price
           as overwritten_later
from proposed p
left join {{ ref('stg_apply_outcome') }} o
    on o.cycle_id = p.cycle_id and o.target = p.target and o.product_id = p.product_id
   and o.zone_id = p.zone_id and o.store_id = p.store_id and o.promo_id = p.promo_id
left join now_values n
    on n.target = p.target and n.product_id = p.product_id and n.zone_id = p.zone_id
   and n.store_id = p.store_id and n.promo_id = p.promo_id
left join (select toUInt64(cycle_id) as cycle_id, store_id, product_id, price
           from {{ ref('stg_shelf_label_queue') }}) q
    on q.cycle_id = p.cycle_id and q.store_id = p.store_id
   and q.product_id = p.product_id
