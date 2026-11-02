{{ config(order_by='(product_id, zone_id)') }}
-- product x zone demand signals (rates are units per day). Only pairs with sales in the history.
with w as (
    select product_id, zone_id,
           sumIf(units, business_date > {{ as_of() }} - 28) as units_28d,
           sumIf(units, business_date > {{ as_of() }} - 90) as units_90d,
           sumIf(units, business_date > {{ as_of() }} - 365) as units_365d,
           sumIf(units, business_date >= {{ as_of() }} - 365
                        and business_date < {{ as_of() }} - 337) as units_season
    from {{ ref('int_sales_daily') }}
    group by product_id, zone_id
)
select product_id,
       zone_id,
       units_28d,
       units_90d,
       units_365d,
       units_28d / 28 as rate_28,
       units_90d / 90 as rate_90,
       if(units_90d > 0, (units_28d / 28) / (units_90d / 90), null) as velocity_ratio,
       if(units_365d > 0, units_season / (units_365d * 28 / 365), null) as seasonality_index
from w
