{{ config(materialized='table', pre_hook=[], order_by='(product_id, zone_id, valid_from)') }}
-- SCD2 per product x zone from the OLTP price history the zone_price trigger keeps (seed,
-- engine and manual changes alike). Not a dbt snapshot (DECISIONS 24); rebuilt every run.
select history_id,
       product_id,
       zone_id,
       price,
       valid_from,
       valid_to,
       valid_to is null as is_current,
       source,
       cycle_id
from {{ ref('stg_price_history') }}
