-- g07: a shelf label exists only where the shelf price changes, checked against inputs the label
-- model does not derive from. A label fails when its price equals the extracted shelf price
-- (the store's open override, else the zone price of the store's zone), or when no zone price,
-- override or promo price of its key changed in the cycle.
with
labels as (
    select l.store_id as store_id, l.product_id as product_id, s.zone_id as zone_id,
           l.price as price
    from {{ ref('pricing_shelf_label') }} l
    inner join {{ ref('stg_store') }} s on s.store_id = l.store_id
    where l.cycle_id = {{ cycle_id() }}
),
snapshot as (
    select lb.store_id as store_id, lb.product_id as product_id,
           if(o.store_id is null, z.price, o.price) as shelf_price
    from labels lb
    left join (
        select store_id, product_id, price from {{ ref('stg_store_price_override') }}
        where valid_to is null
    ) o on o.store_id = lb.store_id and o.product_id = lb.product_id
    left join {{ ref('stg_zone_price') }} z
        on z.product_id = lb.product_id and z.zone_id = lb.zone_id
),
changed_keys as (
    select lb.store_id as store_id, lb.product_id as product_id
    from labels lb
    inner join (
        select product_id, zone_id from {{ ref('pricing_zone_price') }}
        where cycle_id = {{ cycle_id() }} and changed
        union distinct
        select product_id, zone_id from {{ ref('pricing_promo_price') }}
        where cycle_id = {{ cycle_id() }} and changed
    ) c on c.product_id = lb.product_id and c.zone_id = lb.zone_id
    union distinct
    select store_id, product_id from {{ ref('pricing_store_override') }}
    where cycle_id = {{ cycle_id() }} and changed
)
select 'price equals the extracted shelf price' as problem, lb.store_id, lb.product_id, lb.price
from labels lb
inner join snapshot s on s.store_id = lb.store_id and s.product_id = lb.product_id
where s.shelf_price is null or lb.price = s.shelf_price
union all
select 'no price of the key changed in the cycle', store_id, product_id, price
from labels
where (store_id, product_id) not in (select store_id, product_id from changed_keys)
