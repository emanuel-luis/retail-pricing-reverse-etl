{#
  One ladder pass of one kind (PACK or TIER) over every row of `prev`, as an arrayFold per group
  and zone over the members ordered by rank. Raises only, onto the ending grid, each member
  inside its own ceiling `hi` (DECISIONS 39 semantics, now bounded). Rows outside any group of
  this kind form singleton groups and pass through unchanged, so `prev` is read once.

  prev:    relation (product_id, zone_id, p, hi, bad), p and hi in Int64 cents
  members: relation (ladder_group_id, ladder_type, product_id, rank, pack_h)
  Returns a SELECT of (product_id, zone_id, p, hi, bad); bad = a member of the group could not
  be raised enough inside its ceiling (or was already bad).

  Fold elements x = (rank, product_id, p, hi, pack_h, bad). `arrayMap(v -> ..., [value])[1]`
  binds an intermediate value once (lambdas have no let).

  PACK: unit price p / pack non-increasing by rank. Ranks are walked from the largest pack down;
  a member is raised to the smallest grid price whose unit price reaches the highest unit price
  of the strictly larger packs (exact fractions n / d). State (best_n, best_d, rank, rank_n,
  rank_d, out, bad).
  TIER: price strictly increasing by rank (at least one grid step). Ranks are walked upwards; a
  member at or below the highest price of the lower tiers goes to the next grid price above it.
  State (best, rank, rank_max, out, bad).
#}
{% macro ladder_pass(kind, prev, members, grid) -%}
{%- if kind == 'PACK' -%}
    {%- set fold -%}
    arrayFold((acc, x) -> arrayMap(b -> arrayMap(np -> tuple(
                b.1, b.2, x.1,
                if(b.3 * x.5 >= np * b.4, b.3, np),
                if(b.3 * x.5 >= np * b.4, b.4, x.5),
                arrayPushBack(acc.6, np),
                toUInt8(assumeNotNull(acc.7 or np = 0 or np > x.4))),
            [toInt64(if(x.3 >= if(b.1 = 0, 0, intDiv(b.1 * x.5 + b.2 - 1, b.2)), x.3,
                        arrayFirst(g -> g >= if(b.1 = 0, 0, intDiv(b.1 * x.5 + b.2 - 1, b.2)),
                                   grid)))])[1],
        -- b = (best_n, best_d, rank_n, rank_d) after a possible rank change
        [if(x.1 != acc.3,
            tuple(if(acc.1 * acc.5 >= acc.4 * acc.2, acc.1, acc.4),
                  if(acc.1 * acc.5 >= acc.4 * acc.2, acc.2, acc.5), toInt64(0), toInt64(1)),
            tuple(acc.1, acc.2, acc.4, acc.5))])[1],
        members,
        tuple(toInt64(0), toInt64(1), toInt64(-1), toInt64(0), toInt64(1),
              CAST([], 'Array(Int64)'), toUInt8(0)))
    {%- endset -%}
    {%- set sort = "arrayReverseSort" -%}{%- set out_idx = 6 -%}{%- set bad_idx = 7 -%}
{%- else -%}
    {%- set fold -%}
    arrayFold((acc, x) -> arrayMap(b -> arrayMap(np -> tuple(
                b.1, x.1, greatest(b.2, np), arrayPushBack(acc.4, np),
                toUInt8(assumeNotNull(acc.5 or np = 0 or np > x.4))),
            [toInt64(if(b.1 = 0 or x.3 > b.1, x.3, arrayFirst(g -> g > b.1, grid)))])[1],
        -- b = (best, rank_max) after a possible rank change
        [if(x.1 != acc.2, tuple(greatest(acc.1, acc.3), toInt64(0)), tuple(acc.1, acc.3))])[1],
        members,
        tuple(toInt64(0), toInt64(-1), toInt64(0), CAST([], 'Array(Int64)'), toUInt8(0)))
    {%- endset -%}
    {%- set sort = "arraySort" -%}{%- set out_idx = 4 -%}{%- set bad_idx = 5 -%}
{%- endif -%}
select mp.1 as product_id, zone_id, mp.2 as p, mp.3 as hi, toUInt8(mp.4 or group_bad) as bad
from (
    select zone_id,
           arrayZip(arrayMap(x -> x.2, members), st.{{ out_idx }},
                    arrayMap(x -> x.4, members), arrayMap(x -> x.6, members)) as member_prices,
           st.{{ bad_idx }} as group_bad
    from (
        select zone_id, members, {{ fold }} as st
        from (
            select coalesce(toInt64(m.ladder_group_id), -b.product_id) as group_key,
                   b.zone_id as zone_id,
                   (select grid from {{ grid }}) as grid,
                   {{ sort }}(x -> (x.1, x.2), groupArray(tuple(
                       toInt64(coalesce(m.rank, 0)), b.product_id, toInt64(b.p), toInt64(b.hi),
                       toInt64(coalesce(m.pack_h, 100)), toUInt8(b.bad)))) as members
            from {{ prev }} b
            left join (select * from {{ members }} where ladder_type = '{{ kind }}') m
                on m.product_id = b.product_id
            group by group_key, zone_id
        )
    )
)
array join member_prices as mp
{%- endmacro %}
