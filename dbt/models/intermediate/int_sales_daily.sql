{{ config(order_by='(product_id, zone_id, business_date)') }}
-- product x zone x business_date over history_days up to as_of. Returns are subtracted on their
-- own business date. Store sales take the store's zone, web sales the WEB zone. The big table
-- (sale lines) stays on the left of each join; the right sides are the hash tables.
with
sale as (
    select h.sale_id as sale_id,
           h.business_date as business_date,
           if(h.channel = 'WEB',
              (select min(zone_id) from {{ ref('stg_price_zone') }} where channel = 'WEB'),
              st.zone_id) as zone_id
    from {{ ref('stg_sale') }} h
    left join {{ ref('stg_store') }} st on st.store_id = h.store_id
),
lines as (
    select l.product_id as product_id, s.zone_id as zone_id, s.business_date as business_date,
           toInt64(sum(l.quantity)) as units,
           toDecimal128(sum(l.net_amount), 2) as net_revenue,
           toDecimal128(sum(l.quantity * l.unit_cost), 2) as cost
    from {{ ref('stg_sale_line') }} l
    inner join (
        select sale_id, zone_id, business_date from sale
        where business_date > {{ as_of() }} - {{ var('history_days') }}
          and business_date <= {{ as_of() }}
    ) s on s.sale_id = l.sale_id
    group by l.product_id, s.zone_id, s.business_date
),
-- Returns in the window, and only the sale lines and sales they point at: the IN filters are
-- on the primary keys, so the 34M-line table at SF10 is read only where a return exists.
returns_in_window as (
    select sale_line_id, quantity, amount, business_date from {{ ref('stg_sale_return') }}
    where business_date > {{ as_of() }} - {{ var('history_days') }}
      and business_date <= {{ as_of() }}
),
returned_lines as (
    select sale_line_id, sale_id, product_id, unit_cost from {{ ref('stg_sale_line') }}
    where sale_line_id in (select sale_line_id from returns_in_window)
),
returns as (
    select l.product_id as product_id, s.zone_id as zone_id, r.business_date as business_date,
           -toInt64(sum(r.quantity)) as units,
           -toDecimal128(sum(r.amount), 2) as net_revenue,
           -toDecimal128(sum(r.quantity * l.unit_cost), 2) as cost
    from returns_in_window r
    inner join returned_lines l on l.sale_line_id = r.sale_line_id
    inner join (
        select sale_id, zone_id from sale
        where sale_id in (select sale_id from returned_lines)
    ) s on s.sale_id = l.sale_id
    group by l.product_id, s.zone_id, r.business_date
)
select u.product_id as product_id,
       toInt16(assumeNotNull(u.zone_id)) as zone_id,
       u.business_date as business_date,
       toInt64(sum(u.units)) as units,
       toDecimal128(sum(u.net_revenue), 2) as net_revenue,
       toDecimal128(sum(u.cost), 2) as cost,
       if(sum(u.units) > 0, toDecimal128(sum(u.net_revenue), 6) / sum(u.units), null)
           as avg_net_unit_price
from (select * from lines union all select * from returns) u
where u.zone_id is not null
group by u.product_id, u.zone_id, u.business_date
