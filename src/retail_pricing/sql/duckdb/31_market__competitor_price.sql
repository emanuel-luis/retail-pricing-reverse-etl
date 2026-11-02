-- All KVI plus 25% of products; each competitor in ~70% of zones; weekly for 8 weeks up to
-- the anchor. Price = seed zone price * (1 + bias + noise +-3%), ending .x9.
INSERT INTO market.competitor_price BY NAME
WITH prods AS (
    SELECT product_id FROM master.product
    WHERE product_id IN (SELECT product_id FROM rules.kvi) OR u(product_id, 'competitor_watch') < 0.25
),
presence AS (
    SELECT c.competitor_id, c.price_bias, z.zone_id
    FROM market.competitor c CROSS JOIN master.price_zone z
    WHERE u(c.competitor_id || ':' || z.zone_id, 'competitor_zone') < 0.7
),
obs AS (
    SELECT pr.competitor_id, zp.product_id, pr.zone_id, anchor() - CAST(7 * w AS INTEGER) AS observed_date,
           zp.price * (1 + pr.price_bias + 0.06 * (u(pr.competitor_id || ':' || zp.product_id
               || ':' || pr.zone_id || ':' || w, 'competitor_noise') - 0.5)) AS v
    FROM presence pr
    JOIN pricing.zone_price zp USING (zone_id)
    JOIN prods USING (product_id)
    CROSS JOIN range(8) r(w)
)
SELECT row_number() OVER (ORDER BY competitor_id, product_id, zone_id, observed_date)
           AS observation_id,
       competitor_id, product_id, zone_id, observed_date,
       CAST(floor(v * 10) AS INTEGER) * 0.1 + 0.09 AS price
FROM obs;
