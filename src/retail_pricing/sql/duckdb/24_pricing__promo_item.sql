-- The promotion's TPC-DS item plus 4..19 more products of the same class, picked by u.
INSERT INTO pricing.promo_item BY NAME
WITH main AS (
    SELECT pr.p_promo_sk AS promo_id, m.product_id, p.class_id
    FROM tpcds.promotion pr
    JOIN build.item_map m ON m.item_sk = pr.p_item_sk
    JOIN master.product p USING (product_id)
),
extra AS (
    SELECT main.promo_id, p.product_id,
           row_number() OVER (
               PARTITION BY main.promo_id
               ORDER BY u(main.promo_id || ':' || p.product_id, 'promo_item'), p.product_id) AS rn
    FROM main JOIN master.product p ON p.class_id = main.class_id AND p.product_id <> main.product_id
)
SELECT promo_id, product_id FROM main
UNION ALL
SELECT promo_id, product_id FROM extra
WHERE rn <= 4 + floor(16 * u(promo_id, 'promo_size'));
