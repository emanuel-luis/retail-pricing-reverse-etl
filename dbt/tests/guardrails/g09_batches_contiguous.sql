-- g09: per target, batches are numbered 0..n-1 without gaps and none holds more than its size.
{%- set targets = [('rev_zone_price', var('batch_size')),
                   ('rev_store_override', var('batch_size')),
                   ('rev_promo_price', var('batch_size')),
                   ('rev_shelf_label', var('label_batch_size'))] %}
with b as (
    {%- for t, size in targets %}
    select '{{ t }}' as target, batch_no, count() as n, toUInt64({{ size }}) as size
    from {{ ref(t) }} where cycle_id = {{ cycle_id() }} group by batch_no
    {%- if not loop.last %}
    union all
    {%- endif %}
    {%- endfor %}
)
select target, min(batch_no) as first_batch, max(batch_no) as last_batch,
       count() as batches, max(n) as largest, any(size) as batch_size
from b
group by target
having min(batch_no) != 0 or max(batch_no) + 1 != count() or max(n) > any(size)
