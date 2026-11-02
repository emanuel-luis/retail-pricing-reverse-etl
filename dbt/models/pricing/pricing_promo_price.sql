{{ config(order_by='(cycle_id, promo_id, product_id, zone_id)') }}
-- cycle x promo x product x zone: promo prices of active or upcoming promotions (within
-- label_lead_days) whose regular zone price changed, in keep_depth categories. The depth is
-- kept: new_promo = new_regular * (1 - depth), truncated to cents, then rounded down to the grid
-- (DECISIONS 38). Rows with no grid price below the new regular price are left out.
with p as (
    select pp.promo_id as promo_id,
           pp.product_id as product_id,
           pp.zone_id as zone_id,
           pp.promo_price as old_price,
           pp.version as expected_version,
           pr.discount_pct as discount_pct,
           (select grid from {{ ref('int_price_grid') }}) as grid,
           toInt64(zp.new_price * 100) as regular_new_c,
           {{ grid_floor('intDiv(toInt64(zp.new_price * 100) * (10000 - ' ~ e4('pr.discount_pct') ~ '), 10000)') }}
               as new_c
    from {{ ref('stg_promo_price') }} pp
    inner join {{ ref('stg_promotion') }} pr on pr.promo_id = pp.promo_id
    inner join (
        select * from {{ ref('pricing_zone_price') }}
        where cycle_id = {{ cycle_id() }} and changed and promo_policy = 'keep_depth'
    ) zp on zp.product_id = pp.product_id and zp.zone_id = pp.zone_id
    where pr.start_date <= {{ as_of() }} + {{ var('label_lead_days') }}
      and pr.end_date >= {{ as_of() }}
)
select {{ cycle_id() }} as cycle_id,
       promo_id,
       product_id,
       zone_id,
       old_price,
       {{ cents_to_price('new_c') }} as new_price,
       {{ cents_to_price('regular_new_c') }} as regular_new_price,
       discount_pct,
       new_price != old_price as changed,
       'PROMO_DEPTH_KEEP' as primary_reason,
       ['PROMO_DEPTH_KEEP'] as reasons,
       toUInt64(expected_version) as expected_version
from p
where new_c > 0 and new_c < regular_new_c
