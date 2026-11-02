-- PACK: (class, brand) with >= 2 distinct pack_units, rank = dense rank of pack_units.
-- TIER: per (class, size) one product per brand tier (smallest u), groups with >= 2 tiers,
-- rank = tier. Products without a brand are in neither.
CREATE TABLE build.ladder AS
WITH p AS (
    SELECT p.product_id, p.class_id, p.brand_id, p.size, p.pack_units, b.brand_tier
    FROM master.product p JOIN master.brand b USING (brand_id)
),
pack AS (
    SELECT class_id, brand_id FROM p GROUP BY ALL HAVING count(DISTINCT pack_units) >= 2
),
tier_pick AS (
    SELECT * FROM p WHERE size IS NOT NULL
    QUALIFY row_number() OVER (
        PARTITION BY class_id, size, brand_tier
        ORDER BY u(product_id, 'tier_pick'), product_id) = 1
),
tier AS (SELECT class_id, size FROM tier_pick GROUP BY ALL HAVING count(*) >= 2),
members AS (
    SELECT 'PACK' AS ladder_type, p.class_id, p.brand_id, NULL::VARCHAR AS size, p.product_id,
           dense_rank() OVER (PARTITION BY p.class_id, p.brand_id ORDER BY p.pack_units) AS rank
    FROM p JOIN pack USING (class_id, brand_id)
    UNION ALL
    SELECT 'TIER', t.class_id, NULL, t.size, t.product_id, t.brand_tier
    FROM tier_pick t JOIN tier USING (class_id, size)
)
SELECT dense_rank() OVER (ORDER BY ladder_type, class_id, brand_id, size) AS ladder_group_id, *
FROM members;

INSERT INTO rules.ladder_group BY NAME
SELECT DISTINCT ladder_group_id, ladder_type, class_id, brand_id, size FROM build.ladder;
