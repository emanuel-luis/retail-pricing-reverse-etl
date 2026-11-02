{{ config(order_by='category_id') }}
-- category: pooled log-log slope after demeaning ln(price) and ln(units + 1) within product x
-- zone. Clamped to [-3.0, -0.3]; -1.2 when there are too few observations or no price variance.
-- Rounded to 6 decimals and cast to Decimal so no float reaches a price.
with obs as (
    select category_id, product_id, zone_id,
           ln(toFloat64(avg_price)) as x,
           ln(units + 1) as y
    from {{ ref('int_weekly_price_qty') }}
    where units > 0 and avg_price > 0
),
demeaned as (
    select category_id,
           x - avg(x) over (partition by product_id, zone_id) as dx,
           y - avg(y) over (partition by product_id, zone_id) as dy
    from obs
),
fit as (
    select category_id,
           count() as n_obs,
           varPop(dx) as var_x,
           if(varPop(dx) > 0, covarPop(dx, dy) / varPop(dx), null) as raw_slope
    from demeaned
    group by category_id
)
select c.category_id as category_id,
       toUInt64(coalesce(f.n_obs, 0)) as n_obs,
       f.raw_slope as raw_slope,
       coalesce(f.n_obs, 0) < {{ var('elasticity_min_obs') }} or coalesce(f.var_x, 0) < 1e-6
           as is_fallback,
       if(is_fallback, toDecimal64('-1.2', 6),
          toDecimal64(round(least(-0.3, greatest(-3.0, assumeNotNull(f.raw_slope))), 6), 6))
           as elasticity
from {{ ref('stg_category') }} c
left join fit f on f.category_id = c.category_id
