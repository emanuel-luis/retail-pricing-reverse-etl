-- promo_item x zone: regular price * (1 - discount_pct), rounded down to the ending grid.
-- Rows with no grid price below the regular price are dropped and counted.
CREATE TABLE build.promo_price AS
SELECT pi.promo_id, pi.product_id, zp.zone_id, zp.price,
       grid_floor(zp.price * (1 - pr.discount_pct)) AS promo_price
FROM pricing.promo_item pi
JOIN pricing.promotion pr USING (promo_id)
JOIN pricing.zone_price zp USING (product_id);

INSERT INTO build.dropped
SELECT 'pricing.promo_price', 'no_grid_price_below_regular', count(*)
FROM build.promo_price
WHERE promo_price IS NULL OR promo_price >= price
HAVING count(*) > 0;

INSERT INTO pricing.promo_price BY NAME
SELECT promo_id, product_id, zone_id, promo_price
FROM build.promo_price
WHERE promo_price < price;
