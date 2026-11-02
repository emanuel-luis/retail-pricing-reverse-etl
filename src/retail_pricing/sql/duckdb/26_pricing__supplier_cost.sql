-- 24 monthly points (first of month) up to the anchor's month. The base is the wholesale cost
-- of the item version active at that date, with drift +0.2%/month, noise +-1% and, for 10% of
-- products, a 5..10% supplier jump from a random month. The cost is capped at 0.85 * the
-- version's price and at 98% of the cost that keeps the minimum margin at the lowest seed zone
-- price, except for 3% of products, seeded 3 points below the minimum margin at the highest
-- zone price (so every zone breaches it).
INSERT INTO pricing.supplier_cost BY NAME
WITH months AS (
    SELECT k, CAST(date_trunc('month', anchor()) - to_months(CAST(23 - k AS INTEGER)) AS DATE)
               AS valid_from
    FROM range(24) r(k)
),
ver AS (
    SELECT m.product_id, i.i_item_sk, shift_date(i.i_rec_start_date) AS vs,
           i.i_wholesale_cost AS wc, i.i_current_price AS cp
    FROM tpcds.item i JOIN build.item_map m ON m.item_sk = i.i_item_sk
),
pick AS (
    SELECT p.product_id, p.supplier_id, p.class_id, mo.k, mo.valid_from, v.wc, v.cp
    FROM master.product p
    CROSS JOIN months mo
    JOIN ver v USING (product_id)
    QUALIFY row_number() OVER (
        PARTITION BY p.product_id, mo.k
        ORDER BY (v.vs <= mo.valid_from) DESC,
                 CASE WHEN v.vs <= mo.valid_from THEN v.vs END DESC NULLS LAST,
                 v.vs, v.i_item_sk DESC) = 1
),
seed AS (
    SELECT product_id, min(price) AS min_price, max(price) AS max_price
    FROM pricing.zone_price GROUP BY 1
),
c AS (
    SELECT pick.*, r.min_margin, seed.min_price, seed.max_price,
           u(pick.product_id, 'cost_below_min') < 0.03 AS below_min,
           u(pick.product_id, 'cost_jump') < 0.10
               AND pick.k >= floor(24 * u(pick.product_id, 'cost_jump_month')) AS jumped,
           0.05 + 0.05 * u(pick.product_id, 'cost_jump_size') AS jump_size,
           1 + 0.02 * (u(pick.product_id || ':' || pick.k, 'cost_noise') - 0.5) AS noise
    FROM pick
    JOIN master.product_class pc USING (class_id)
    JOIN rules.category_rule r USING (category_id)
    JOIN seed USING (product_id)
)
SELECT product_id, valid_from, supplier_id,
       CASE WHEN below_min
            THEN max_price * (1 - min_margin + 0.03) * pow(1.002, k - 23)
            ELSE least(wc * pow(1.002, k) * noise * CASE WHEN jumped THEN 1 + jump_size ELSE 1 END,
                       0.85 * cp,
                       0.98 * (1 - min_margin) * min_price)
       END AS unit_cost
FROM c;
