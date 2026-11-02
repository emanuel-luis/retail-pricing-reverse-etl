-- Assortment = (store, product) sold in the last 365 days. on_hand = round(avg daily units
-- over 90 days * cover), cover = 3 + 87u days; last received = anchor - floor(180u).
INSERT INTO inventory.store_stock BY NAME
WITH sold AS (
    SELECT h.store_id, l.product_id,
           coalesce(sum(l.quantity) FILTER (WHERE h.business_date > anchor() - 90), 0) AS units_90
    FROM sales.sale_line l JOIN sales.sale h USING (sale_id)
    WHERE h.channel = 'STORE' AND h.business_date > anchor() - 365
    GROUP BY ALL
)
SELECT store_id, product_id,
       CAST(round(units_90 / 90 * (3 + 87 * u(store_id || ':' || product_id, 'cover'))) AS INTEGER)
           AS on_hand,
       anchor() - CAST(floor(180 * u(store_id || ':' || product_id, 'received')) AS INTEGER)
           AS last_received_date
FROM sold;
