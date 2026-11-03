{{ config(order_by='(cycle_id, target, zone_id, category_id, check_name)') }}
-- cycle x target x zone x category: the transit check of pricing_apply.reconcile_actual, from the
-- rev rows: rows, sum of price cents and sum(((k1 * 1000003 + k2) % 1000000007) * cents) with
-- the keys of DECISIONS 64. Integer-exact: the weighted sum is Int128 (it can pass Int64).
with sent as (
    select 'zone_price' as target, zone_id, product_id, toInt64(product_id) as k1,
           toInt64(zone_id) as k2, new_price as price
    from {{ ref('rev_zone_price') }} where cycle_id = {{ cycle_id() }}
    union all
    select 'store_override', zone_id, product_id, toInt64(store_id), toInt64(product_id),
           new_price
    from {{ ref('rev_store_override') }} where cycle_id = {{ cycle_id() }}
    union all
    select 'promo_price', zone_id, product_id, toInt64(promo_id),
           toInt64(product_id) * 100 + zone_id, new_price
    from {{ ref('rev_promo_price') }} where cycle_id = {{ cycle_id() }}
    union all
    select 'shelf_label', zone_id, product_id, toInt64(store_id), toInt64(product_id), price
    from {{ ref('rev_shelf_label') }} where cycle_id = {{ cycle_id() }}
)
select {{ cycle_id() }} as cycle_id,
       r.target as target,
       toInt16(r.zone_id) as zone_id,
       toInt32(pc.category_id) as category_id,
       'transit' as check_name,
       toInt64(count()) as rows,
       toInt64(sum(toInt64(r.price * 100))) as sum_cents,
       sum(toInt128((r.k1 * 1000003 + r.k2) % 1000000007) * toInt128(r.price * 100)) as wsum
from sent r
inner join {{ ref('stg_product') }} p on p.product_id = r.product_id
inner join {{ ref('stg_product_class') }} pc on pc.class_id = p.class_id
group by r.target, r.zone_id, pc.category_id
