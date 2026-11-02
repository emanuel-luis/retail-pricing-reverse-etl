-- Last weekly snapshot per (warehouse, product); item versions summed, null quantity = 0.
INSERT INTO inventory.warehouse_stock BY NAME
WITH inv AS (
    SELECT i.inv_warehouse_sk AS warehouse_id, m.product_id, i.inv_date_sk,
           coalesce(i.inv_quantity_on_hand, 0) AS qty
    FROM tpcds.inventory i JOIN build.item_map m ON m.item_sk = i.inv_item_sk
),
last AS (SELECT warehouse_id, product_id, max(inv_date_sk) AS inv_date_sk FROM inv GROUP BY ALL)
SELECT warehouse_id, product_id, sum(qty) AS on_hand
FROM inv JOIN last USING (warehouse_id, product_id, inv_date_sk)
GROUP BY ALL;
