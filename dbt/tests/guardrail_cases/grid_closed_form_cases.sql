-- The closed-form grid macros against an enumeration of every grid price, for each cent from 0
-- to 600.00 on the built int_price_grid. Fails (returns rows) on the first cents where
-- grid_floor, grid_ceil, grid_next_above or grid_round differs from the enumerated answer
-- (floor: largest <= v, ceil: smallest >= v, next: smallest > v, round: nearest, a tie up).
-- The reference uses running windows over the cents (a grid array per row would cost memory).
with
points as (
    select arrayJoin(arrayFlatten(arrayMap((e, f, t) ->
               arrayMap(n -> toInt64(n * 100 + e),
                        range(toUInt64(intDiv(f - e, 100)),
                              toUInt64(intDiv(least(t, 70000) - e, 100) + 1))),
               g.1, g.2, g.3))) as p
    from (select {{ grid_value() }} as g)
),
cents as (
    select toInt64(number) as v, v in (select p from points) as is_point
    from numbers(70001)
),
reference as (
    select v,
           max(if(is_point, v, 0)) over (order by v rows between unbounded preceding and current row)
               as floor_old,
           min(if(is_point, v, 9223372036854775807))
               over (order by v rows between current row and unbounded following) as ceil_any,
           min(if(is_point, v, 9223372036854775807))
               over (order by v rows between 1 following and unbounded following) as next_any
    from cents
),
checked as (
    select v,
           {{ grid_value() }} as grid,
           floor_old,
           if(ceil_any = 9223372036854775807, 0, ceil_any) as ceil_old,
           if(next_any = 9223372036854775807, 0, next_any) as next_old,
           multiIf(floor_old = 0, ceil_old, ceil_old = 0, floor_old,
                   ceil_old - v <= v - floor_old, ceil_old, floor_old) as round_old,
           {{ grid_floor('v') }} as floor_new,
           {{ grid_ceil('v') }} as ceil_new,
           {{ grid_next_above('v') }} as next_new,
           {{ grid_round('v') }} as round_new
    from reference
    where v <= 60000
)
select v, floor_new, floor_old, ceil_new, ceil_old, next_new, next_old, round_new, round_old
from checked
where floor_new != floor_old or ceil_new != ceil_old or next_new != next_old
   or round_new != round_old
order by v
limit 20
