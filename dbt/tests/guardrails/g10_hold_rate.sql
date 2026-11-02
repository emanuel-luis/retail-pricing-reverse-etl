{{ config(severity='warn') }}
-- g10 (warn): at most 20% of the cycle's zone prices are held.
select countIf(primary_reason like 'HOLD_%') as held, count() as total
from {{ ref('pricing_zone_price') }}
where cycle_id = {{ cycle_id() }}
having held > 0.2 * total
