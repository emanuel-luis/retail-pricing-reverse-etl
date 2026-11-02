{{ config(order_by='(product_id, zone_id)') }}
-- product x zone: the extracted zone price and version plus everything the rules read about
-- the pair: category rule, KVI, the active or upcoming promotion (deepest, then lowest id),
-- open store overrides in the zone and ladder membership.
with
promo as (
    select pi.product_id as product_id, z.zone_id as zone_id,
           argMax(pr.promo_id, (pr.discount_pct, -pr.promo_id)) as promo_id,
           max(pr.discount_pct) as promo_discount_pct
    from {{ ref('stg_promo_item') }} pi
    inner join (
        select promo_id, discount_pct,
               arrayJoin(if(channel = 'ALL', ['STORE', 'WEB'], [channel])) as zone_channel
        from {{ ref('stg_promotion') }}
        where start_date <= {{ as_of() }} + {{ var('label_lead_days') }}
          and end_date >= {{ as_of() }}
    ) pr on pr.promo_id = pi.promo_id
    inner join {{ ref('stg_price_zone') }} z on z.channel = pr.zone_channel
    group by pi.product_id, z.zone_id
),
overrides as (
    select o.product_id as product_id, s.zone_id as zone_id, count() as override_count
    from {{ ref('stg_store_price_override') }} o
    inner join {{ ref('stg_store') }} s on s.store_id = o.store_id
    where o.valid_to is null
    group by o.product_id, s.zone_id
),
ladders as (
    select m.product_id as product_id,
           max(g.ladder_type = 'PACK') as in_pack_ladder,
           max(g.ladder_type = 'TIER') as in_tier_ladder
    from {{ ref('stg_ladder_member') }} m
    inner join {{ ref('stg_ladder_group') }} g on g.ladder_group_id = m.ladder_group_id
    group by m.product_id
)
select zp.product_id as product_id,
       zp.zone_id as zone_id,
       z.channel as zone_channel,
       p.class_id as class_id,
       pc.category_id as category_id,
       p.brand_id as brand_id,
       p.pack_units as pack_units,
       zp.price as current_price,
       zp.version as current_version,
       pr.promo_id as promo_id,
       pr.promo_discount_pct as promo_discount_pct,
       pr.promo_id is not null as has_promo,
       toUInt64(coalesce(o.override_count, 0)) as override_count,
       cr.target_margin as target_margin,
       cr.min_margin as min_margin,
       cr.max_change_pct as max_change_pct,
       cr.cover_min_days as cover_min_days,
       cr.cover_max_days as cover_max_days,
       cr.aging_max_days as aging_max_days,
       cr.promo_policy as promo_policy,
       k.kvi_tier as kvi_tier,
       k.competitor_index_target as competitor_index_target,
       coalesce(l.in_pack_ladder, 0) = 1 as in_pack_ladder,
       coalesce(l.in_tier_ladder, 0) = 1 as in_tier_ladder
from {{ ref('stg_zone_price') }} zp
inner join {{ ref('stg_product') }} p on p.product_id = zp.product_id
inner join {{ ref('stg_product_class') }} pc on pc.class_id = p.class_id
inner join {{ ref('stg_price_zone') }} z on z.zone_id = zp.zone_id
left join {{ ref('stg_category_rule') }} cr on cr.category_id = pc.category_id
left join {{ ref('stg_kvi') }} k on k.product_id = zp.product_id
left join promo pr on pr.product_id = zp.product_id and pr.zone_id = zp.zone_id
left join overrides o on o.product_id = zp.product_id and o.zone_id = zp.zone_id
left join ladders l on l.product_id = zp.product_id
