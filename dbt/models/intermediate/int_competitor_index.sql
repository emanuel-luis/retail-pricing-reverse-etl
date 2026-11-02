{{ config(order_by='(product_id, zone_id)') }}
-- product x zone: exact median competitor price over the last 14 days (28 days when the 14-day
-- window is empty), number of competitors and our current price index against it.
with w as (
    select product_id, zone_id,
           countIf(observed_date > {{ as_of() }} - 14) as n_obs_14,
           (toDecimal64(quantileExactLowIf(0.5)(price, observed_date > {{ as_of() }} - 14), 4)
            + toDecimal64(quantileExactHighIf(0.5)(price, observed_date > {{ as_of() }} - 14), 4))
               / 2 as median_14,
           (toDecimal64(quantileExactLow(0.5)(price), 4)
            + toDecimal64(quantileExactHigh(0.5)(price), 4)) / 2 as median_28,
           uniqExactIf(competitor_id, observed_date > {{ as_of() }} - 14) as n_competitors_14,
           uniqExact(competitor_id) as n_competitors_28
    from {{ ref('stg_competitor_price') }}
    where observed_date > {{ as_of() }} - 28 and observed_date <= {{ as_of() }}
    group by product_id, zone_id
)
select w.product_id as product_id,
       w.zone_id as zone_id,
       toUInt8(if(w.n_obs_14 > 0, 14, 28)) as window_days,
       if(w.n_obs_14 > 0, w.median_14, w.median_28) as competitor_median,
       toUInt64(if(w.n_obs_14 > 0, w.n_competitors_14, w.n_competitors_28)) as n_competitors,
       toDecimal128(c.current_price, 6) / competitor_median as price_index
from w
inner join {{ ref('int_current_state') }} c on c.product_id = w.product_id and c.zone_id = w.zone_id
