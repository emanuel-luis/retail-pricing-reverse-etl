-- Current i_current_price * zone price_factor, rounded to the ending grid. build.py then
-- raises prices (synth.repair_zone) until every PACK and TIER ladder holds.
INSERT INTO pricing.zone_price BY NAME
SELECT p.product_id,
       z.zone_id,
       grid_round(i.i_current_price * z.price_factor) AS price,
       coalesce(shift_date(i.i_rec_start_date), anchor()) AS effective_from
FROM master.product p
JOIN build.item_current i ON i.i_item_id = p.product_code
CROSS JOIN master.price_zone z;
