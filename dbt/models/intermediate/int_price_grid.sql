-- One row: every grid price in cents from rules.price_ending_band (price n + ending inside
-- [min_price, max_price)), up to twice the highest zone price plus 100, the same span the seed
-- build uses. Sorted, so the grid macros can take the first or last match.
with
top as (
    select toInt64((ceil(max(price)) * 2 + 100) * 100) as top_c from {{ ref('stg_zone_price') }}
),
bands as (
    select toInt64(ending * 100) as e_c,
           toInt64(min_price * 100) as min_c,
           if(max_price is null, null, toInt64(max_price * 100)) as max_c
    from {{ ref('stg_price_ending_band') }}
)
select arraySort(arrayDistinct(arrayFlatten(groupArray(
           arrayMap(n -> toInt64(n * 100 + e_c),
                    range(toUInt64(greatest(intDiv(min_c - e_c + 99, 100), 0)),
                          toUInt64(greatest(if(max_c is null, intDiv(top_c, 100) + 1,
                                                intDiv(max_c - e_c + 99, 100)), 0))))
       )))) as grid
from bands
cross join top
