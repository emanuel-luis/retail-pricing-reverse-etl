-- Top 2% of products (ceil, at least 5) by store units in the last 365 days; tier 1 is the
-- top 0.5% (ceil, at least 1). Same counts as synth.kvi_count / kvi_tier1_count.
INSERT INTO rules.kvi BY NAME
WITH units AS (
    SELECT l.product_id, sum(l.quantity) AS units
    FROM sales.sale_line l JOIN sales.sale h USING (sale_id)
    WHERE h.channel = 'STORE' AND h.business_date > anchor() - 365
    GROUP BY 1
),
n AS (SELECT count(*) AS products FROM master.product),
ranked AS (
    SELECT product_id, row_number() OVER (ORDER BY units DESC, product_id) AS rk
    FROM units WHERE units > 0
)
SELECT product_id,
       CASE WHEN rk <= greatest(1, (products + 199) // 200) THEN 1 ELSE 2 END AS kvi_tier,
       CASE WHEN rk <= greatest(1, (products + 199) // 200) THEN 0.98 ELSE 1.00 END
           AS competitor_index_target
FROM ranked, n
WHERE rk <= greatest(5, (2 * products + 99) // 100);
