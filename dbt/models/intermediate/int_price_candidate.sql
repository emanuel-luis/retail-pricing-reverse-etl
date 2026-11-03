{{ config(order_by='(product_id, zone_id)') }}
-- product x zone: the unbounded candidate price (Decimal, no float) and its component reasons.
-- factor = 1 + s * (velocity +-2% | cover -3% overstock or aged, +2% low cover | seasonality
-- +-1%), s = least(1, 1 / |elasticity|). KVI with a competitor: median * index target. Other
-- products with a competitor: 0.7 * current * factor + 0.3 * median * 1.02. Else current * factor.
with
signals as (
    select c.product_id as product_id,
           c.zone_id as zone_id,
           c.zone_channel as zone_channel,
           c.class_id as class_id,
           c.category_id as category_id,
           c.pack_units as pack_units,
           c.current_price as current_price,
           c.current_version as current_version,
           c.has_promo as has_promo,
           c.promo_policy as promo_policy,
           c.min_margin as min_margin,
           c.max_change_pct as max_change_pct,
           c.kvi_tier as kvi_tier,
           c.competitor_index_target as competitor_index_target,
           cc.unit_cost as unit_cost,
           cc.cost_change_90d_pct as cost_change_90d_pct,
           e.elasticity as elasticity,
           d.velocity_ratio as velocity_ratio,
           d.seasonality_index as seasonality_index,
           coalesce(sc.is_overstock, false) as is_overstock,
           coalesce(sc.is_aged, false) as is_aged,
           coalesce(sc.is_low_cover, false) as is_low_cover,
           ci.competitor_median as competitor_median,
           ci.n_competitors as n_competitors,
           least({{ dec(1) }}, {{ dec(1) }} / abs(toDecimal128(e.elasticity, 10))) as s,
           multiIf(coalesce(d.velocity_ratio > 1.2, false), {{ dec('0.02') }},
                   coalesce(d.velocity_ratio < 0.8, false), {{ dec('-0.02') }},
                   {{ dec(0) }}) as w_velocity,
           multiIf(is_overstock or is_aged, {{ dec('-0.03') }},
                   is_low_cover, {{ dec('0.02') }},
                   {{ dec(0) }}) as w_cover,
           multiIf(coalesce(d.seasonality_index > 1.2, false), {{ dec('0.01') }},
                   coalesce(d.seasonality_index < 0.8, false), {{ dec('-0.01') }},
                   {{ dec(0) }}) as w_season
    from {{ ref('int_current_state') }} c
    left join {{ ref('int_cost_current') }} cc on cc.product_id = c.product_id
    left join {{ ref('int_elasticity') }} e on e.category_id = c.category_id
    left join {{ ref('int_demand') }} d on d.product_id = c.product_id and d.zone_id = c.zone_id
    left join {{ ref('int_stock_cover') }} sc
        on sc.product_id = c.product_id and sc.zone_id = c.zone_id
    left join {{ ref('int_competitor_index') }} ci
        on ci.product_id = c.product_id and ci.zone_id = c.zone_id
),
factored as (
    select *,
           toDecimal128({{ dec(1) }} + s * (w_velocity + w_cover + w_season), 10) as factor,
           competitor_median is not null as has_competitor,
           kvi_tier is not null and competitor_median is not null as kvi_aligned
    from signals
),
candidate as (
    select *,
           multiIf(kvi_aligned,
                   toDecimal128(assumeNotNull(competitor_median)
                                * assumeNotNull(competitor_index_target), 10),
                   has_competitor,
                   toDecimal128({{ dec('0.7') }} * current_price * factor
                                + {{ dec('0.3') }} * assumeNotNull(competitor_median)
                                  * {{ dec('1.02') }}, 10),
                   toDecimal128(current_price * factor, 10)) as candidate_price
    from factored
),
components as (
    select *,
           [if(kvi_aligned, '', multiIf(w_velocity > 0, 'VELOCITY_UP',
                                        w_velocity < 0, 'VELOCITY_DOWN', '')),
            if(kvi_aligned, '', multiIf(is_overstock, 'OVERSTOCK_MARKDOWN',
                                        is_aged, 'AGING_MARKDOWN',
                                        is_low_cover, 'LOW_COVER_UPLIFT', '')),
            if(kvi_aligned or w_season = 0, '', 'SEASONAL'),
            multiIf(kvi_aligned, 'COMPETITOR_ALIGN_KVI', has_competitor, 'COMPETITOR_BLEND', '')]
               as all_reasons,
           [toDecimal128(s * abs(w_velocity), 10),
            toDecimal128(s * abs(w_cover), 10),
            toDecimal128(s * abs(w_season), 10),
            toDecimal128(if(has_competitor,
                            abs(candidate_price - if(kvi_aligned, toDecimal128(current_price, 10),
                                                     toDecimal128(current_price * factor, 10)))
                                / current_price,
                            {{ dec(0) }}), 10)] as all_weights
    from candidate
)
select product_id,
       zone_id,
       zone_channel,
       class_id,
       category_id,
       pack_units,
       current_price,
       current_version,
       has_promo,
       promo_policy,
       min_margin,
       max_change_pct,
       kvi_tier,
       unit_cost,
       cost_change_90d_pct,
       elasticity,
       velocity_ratio,
       seasonality_index,
       is_overstock,
       is_aged,
       is_low_cover,
       competitor_median,
       n_competitors,
       factor,
       candidate_price,
       arrayFilter((r, w) -> r != '', all_reasons, all_weights) as component_reasons,
       arrayFilter((w, r) -> r != '', all_weights, all_reasons) as component_weights,
       -- the largest component; the first one in the fixed order above wins a tie
       if(empty(component_reasons), '',
          component_reasons[indexOf(component_weights, arrayMax(component_weights))])
           as top_component
from components
