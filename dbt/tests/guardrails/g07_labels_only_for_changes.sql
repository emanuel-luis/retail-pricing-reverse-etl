-- g07: a shelf label exists only where the effective shelf price changes.
select store_id, product_id, old_price, price
from {{ ref('pricing_shelf_label') }}
where cycle_id = {{ cycle_id() }} and price = old_price
