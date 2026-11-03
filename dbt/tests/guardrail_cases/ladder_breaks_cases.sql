-- depends_on: {{ ref('stg_ladder_member') }}
-- Fixture check of the g03/g04 logic (ladder_breaks) on inline rows. Fails (returns rows) when
-- the macro flags a group it must skip or misses one it must flag:
--   group 1 PACK, broken, every member held (nothing changed): skipped;
--   group 2 PACK, broken, one member changed: flagged;
--   group 3 TIER, broken (equal tiers), nothing changed: skipped;
--   group 4 TIER, broken, one member changed: flagged;
--   group 5 PACK, monotone, one member changed: not flagged.
with
prices as (
    select toInt64(product_id) as product_id, toInt16(1) as zone_id,
           toDecimal64(new_price, 2) as new_price, changed
    from values('product_id UInt32, new_price String, changed Bool',
        (1, '2.49', false), (2, '5.49', false),
        (3, '2.49', true),  (4, '5.49', false),
        (5, '9.99', false), (6, '9.99', false),
        (7, '9.99', false), (8, '9.49', true),
        (9, '2.99', true),  (10, '5.49', false))
),
members as (
    select ladder_group_id, ladder_type, toInt64(product_id) as product_id,
           toInt16(rank) as rank, toDecimal64(pack_units, 2) as pack_units
    from values('ladder_group_id Int32, ladder_type String, product_id UInt32, rank UInt8, pack_units String',
        (1, 'PACK', 1, 1, '1'), (1, 'PACK', 2, 2, '2'),
        (2, 'PACK', 3, 1, '1'), (2, 'PACK', 4, 2, '2'),
        (3, 'TIER', 5, 1, '1'), (3, 'TIER', 6, 2, '1'),
        (4, 'TIER', 7, 1, '1'), (4, 'TIER', 8, 2, '1'),
        (5, 'PACK', 9, 1, '1'), (5, 'PACK', 10, 2, '2'))
),
flagged as (
    select distinct ladder_group_id from ({{ ladder_breaks('PACK', 'prices', 'members') }})
    union distinct
    select distinct ladder_group_id from ({{ ladder_breaks('TIER', 'prices', 'members') }})
),
expected as (select arrayJoin([2, 4]) as ladder_group_id)
select 'unexpected' as problem, ladder_group_id from flagged
where ladder_group_id not in (select ladder_group_id from expected)
union all
select 'missing', ladder_group_id from expected
where ladder_group_id not in (select ladder_group_id from flagged)
