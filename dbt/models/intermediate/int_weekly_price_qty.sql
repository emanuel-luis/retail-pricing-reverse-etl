{{ config(order_by='(product_id, zone_id, week_start)') }}
-- product x zone x ISO week (Monday) over the last 52 weeks: units and average net price.
select d.product_id as product_id,
       d.zone_id as zone_id,
       toMonday(d.business_date) as week_start,
       any(pc.category_id) as category_id,
       toInt64(sum(d.units)) as units,
       if(sum(d.units) > 0, toDecimal128(sum(d.net_revenue), 6) / sum(d.units), null) as avg_price
from {{ ref('int_sales_daily') }} d
inner join {{ ref('stg_product') }} p on p.product_id = d.product_id
inner join {{ ref('stg_product_class') }} pc on pc.class_id = p.class_id
where d.business_date > {{ as_of() }} - 364
group by d.product_id, d.zone_id, week_start
