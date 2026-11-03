{{ config(order_by='(cycle_id, zone_id, category_id, primary_reason)') }}
-- cycle x zone x category x primary_reason over changed zone prices: counts, average change
-- and the projected 28-day revenue and margin delta.
select zp.cycle_id as cycle_id,
       zp.zone_id as zone_id,
       zp.category_id as category_id,
       assumeNotNull(zp.primary_reason) as primary_reason,
       toUInt64(count()) as n_changed,
       toUInt64(countIf(zp.new_price > zp.old_price)) as n_up,
       toUInt64(countIf(zp.new_price < zp.old_price)) as n_down,
       round(avg(toFloat64(zp.new_price) / toFloat64(zp.old_price) - 1), 6) as avg_change_pct,
       round(sum(mp.revenue_delta), 4) as projected_revenue_delta,
       round(sum(mp.margin_delta), 4) as projected_margin_delta
from {{ ref('pricing_zone_price') }} zp
inner join {{ ref('pricing_margin_projection') }} mp
    on mp.cycle_id = zp.cycle_id and mp.product_id = zp.product_id and mp.zone_id = zp.zone_id
where zp.changed
group by zp.cycle_id, zp.zone_id, zp.category_id, zp.primary_reason
