-- g03: in every PACK group that is not held, unit price (price / pack_units) does not rise
-- with the pack size (rank).
with m as (
    select lm.ladder_group_id as ladder_group_id, lm.rank as rank, zp.zone_id as zone_id,
           toInt64(zp.new_price * 100) as p, toInt64(pr.pack_units * 100) as pack_h,
           zp.primary_reason as primary_reason
    from {{ ref('stg_ladder_member') }} lm
    inner join {{ ref('stg_ladder_group') }} g on g.ladder_group_id = lm.ladder_group_id
    inner join {{ ref('stg_product') }} pr on pr.product_id = lm.product_id
    inner join {{ ref('pricing_zone_price') }} zp on zp.product_id = lm.product_id
    where g.ladder_type = 'PACK' and zp.cycle_id = {{ cycle_id() }}
),
held as (select distinct ladder_group_id, zone_id from m where primary_reason = 'HOLD_LADDER')
select a.ladder_group_id, a.zone_id, a.rank, a.p, a.pack_h, b.rank, b.p, b.pack_h
from m a
inner join m b on b.ladder_group_id = a.ladder_group_id and b.zone_id = a.zone_id
where a.rank < b.rank
  and a.p * b.pack_h < b.p * a.pack_h
  and (a.ladder_group_id, a.zone_id) not in (select ladder_group_id, zone_id from held)
