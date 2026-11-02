-- 2% of the store_stock rows of stores with price_adjust_pct > 0: zone price * (1 + adj),
-- rounded to the ending grid; open (valid_to NULL), started up to 90 days before the anchor.
INSERT INTO pricing.store_price_override BY NAME
SELECT row_number() OVER (ORDER BY s.store_id, s.product_id) AS override_id,
       s.store_id,
       s.product_id,
       grid_round(zp.price * (1 + st.price_adjust_pct)) AS price,
       anchor() - CAST(floor(90 * u(s.store_id || ':' || s.product_id, 'override_from')) AS INTEGER)
           AS valid_from,
       NULL AS valid_to
FROM inventory.store_stock s
JOIN master.store st USING (store_id)
JOIN pricing.zone_price zp ON zp.product_id = s.product_id AND zp.zone_id = st.zone_id
WHERE st.price_adjust_pct > 0 AND u(s.store_id || ':' || s.product_id, 'override') < 0.02;
