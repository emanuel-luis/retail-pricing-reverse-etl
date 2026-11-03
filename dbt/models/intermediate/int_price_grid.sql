-- One row: the price-ending grid from rules.price_ending_band in closed form. Per band (sorted
-- by its first price): the ending, the first grid price (n * 100 + ending >= min_price) and the
-- last one (< max_price; 10^15 cents when unbounded), all Int64 cents. The grid macros do
-- arithmetic over these few bands instead of scanning an array of every grid price.
with
bands as (
    select toInt64(ending * 100) as e_c,
           toInt64(min_price * 100) as min_c,
           if(max_price is null, null, toInt64(max_price * 100)) as max_c
    from {{ ref('stg_price_ending_band') }}
),
spans as (
    select e_c,
           greatest(intDiv(min_c - e_c + 99, 100), 0) * 100 + e_c as first_c,
           if(max_c is null, toInt64(1000000000000000),
              (intDiv(assumeNotNull(max_c) - e_c + 99, 100) - 1) * 100 + e_c) as top_c
    from bands
),
sorted as (
    select arraySort(groupArray((first_c, e_c, top_c))) as s from spans where top_c >= first_c
)
select arrayMap(t -> t.2, s) as band_ending,
       arrayMap(t -> t.1, s) as band_first,
       arrayMap(t -> t.3, s) as band_top
from sorted
