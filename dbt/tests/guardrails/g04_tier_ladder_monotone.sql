-- g04: in every TIER group that is not held, price rises strictly with the brand tier (rank).
with m as (
    select lm.ladder_group_id as ladder_group_id, lm.rank as rank, zp.zone_id as zone_id,
           zp.new_price as p, zp.primary_reason as primary_reason
    from {{ ref('stg_ladder_member') }} lm
    inner join {{ ref('stg_ladder_group') }} g on g.ladder_group_id = lm.ladder_group_id
    inner join {{ ref('pricing_zone_price') }} zp on zp.product_id = lm.product_id
    where g.ladder_type = 'TIER' and zp.cycle_id = {{ cycle_id() }}
),
held as (select distinct ladder_group_id, zone_id from m where primary_reason = 'HOLD_LADDER')
select a.ladder_group_id, a.zone_id, a.rank, a.p, b.rank, b.p
from m a
inner join m b on b.ladder_group_id = a.ladder_group_id and b.zone_id = a.zone_id
where a.rank < b.rank
  and a.p >= b.p
  and (a.ladder_group_id, a.zone_id) not in (select ladder_group_id, zone_id from held)
