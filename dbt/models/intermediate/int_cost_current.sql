{{ config(order_by='product_id') }}
-- product: the supplier cost valid at as_of (latest valid_from <= as_of) and its change over
-- 90 days. Products with no cost valid at as_of are absent (HOLD_NO_COST downstream).
select product_id,
       argMaxIf(c.unit_cost, c.valid_from, c.valid_from <= {{ as_of() }}) as unit_cost,
       maxIf(valid_from, valid_from <= {{ as_of() }}) as cost_valid_from,
       if(countIf(valid_from <= {{ as_of() }} - 90) > 0,
          argMaxIf(c.unit_cost, c.valid_from, c.valid_from <= {{ as_of() }} - 90), null)
           as unit_cost_90d_ago,
       if(unit_cost_90d_ago > 0, toDecimal128(unit_cost, 6) / unit_cost_90d_ago - 1, null)
           as cost_change_90d_pct
from {{ ref('stg_supplier_cost') }} c
group by product_id
having countIf(valid_from <= {{ as_of() }}) > 0
