{{ config(order_by='(cycle_id, store_id, product_id)') }}
-- cycle x store x product: a label for every stocked (store, product) whose effective shelf
-- price changes. The effective price is the open override when the store has one, else the zone
-- price; promo_price is the price of the pair's active or upcoming promotion (int_current_state),
-- recalculated when pricing_promo_price has it.
with
zone as (
    select product_id, zone_id, old_price, new_price
    from {{ ref('pricing_zone_price') }} where cycle_id = {{ cycle_id() }}
),
ovr as (
    select store_id, product_id, old_price, new_price
    from {{ ref('pricing_store_override') }} where cycle_id = {{ cycle_id() }}
),
promo as (
    select c.product_id as product_id, c.zone_id as zone_id,
           coalesce(np.new_price, op.promo_price) as promo_price
    from {{ ref('int_current_state') }} c
    left join {{ ref('stg_promo_price') }} op
        on op.promo_id = c.promo_id and op.product_id = c.product_id and op.zone_id = c.zone_id
    left join (
        select promo_id, product_id, zone_id, new_price
        from {{ ref('pricing_promo_price') }} where cycle_id = {{ cycle_id() }}
    ) np on np.promo_id = c.promo_id and np.product_id = c.product_id and np.zone_id = c.zone_id
    where c.has_promo
),
labels as (
    select ss.store_id as store_id,
           ss.product_id as product_id,
           st.zone_id as zone_id,
           o.store_id is not null as has_override,
           if(has_override, o.old_price, z.old_price) as old_price,
           if(has_override, o.new_price, z.new_price) as price
    from {{ ref('stg_store_stock') }} ss
    inner join {{ ref('stg_store') }} st on st.store_id = ss.store_id
    inner join zone z on z.product_id = ss.product_id and z.zone_id = st.zone_id
    left join ovr o on o.store_id = ss.store_id and o.product_id = ss.product_id
)
select {{ cycle_id() }} as cycle_id,
       l.store_id as store_id,
       l.product_id as product_id,
       l.zone_id as zone_id,
       l.old_price as old_price,
       l.price as price,
       p.promo_price as promo_price,
       {{ as_of() }} + {{ var('label_lead_days') }} as due_date,
       if(l.has_override, 'store_override', 'zone_price') as source_target
from labels l
left join promo p on p.product_id = l.product_id and p.zone_id = l.zone_id
where l.price != l.old_price
