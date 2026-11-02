-- One RECEIPT per store_stock row at its last received date; the workload appends the rest.
INSERT INTO inventory.stock_movement BY NAME
SELECT row_number() OVER (ORDER BY store_id, product_id) AS movement_id,
       store_id, product_id, 'RECEIPT' AS movement_type, on_hand AS qty,
       last_received_date AS business_date
FROM inventory.store_stock;
