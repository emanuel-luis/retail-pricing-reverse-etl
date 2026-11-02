-- Target margin 25..45%, min = target - 10 pts, max change 5% (3% when > 5% of the category's
-- products are KVI), cover 7..60 days, aging 120 days, promo policy keep_depth (70%) or exclude.
INSERT INTO rules.category_rule BY NAME
WITH k AS (
    SELECT c.category_id, count(*) AS products, count(k.product_id) AS kvis
    FROM master.product p
    JOIN master.product_class c USING (class_id)
    LEFT JOIN rules.kvi k USING (product_id)
    GROUP BY 1
),
t AS (
    SELECT category_id,
           CAST(25 + floor(21 * u(category_id, 'target_margin')) AS INTEGER) * 0.01 AS target_margin
    FROM master.category
)
SELECT t.category_id,
       t.target_margin,
       t.target_margin - 0.10 AS min_margin,
       CASE WHEN coalesce(k.kvis, 0) * 20 > coalesce(k.products, 0) THEN 0.03 ELSE 0.05 END
           AS max_change_pct,
       7 AS cover_min_days,
       60 AS cover_max_days,
       120 AS aging_max_days,
       CASE WHEN u(t.category_id, 'promo_policy') < 0.7 THEN 'keep_depth' ELSE 'exclude' END
           AS promo_policy
FROM t LEFT JOIN k USING (category_id);
