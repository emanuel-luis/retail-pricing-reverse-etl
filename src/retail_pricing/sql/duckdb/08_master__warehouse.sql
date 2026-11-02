INSERT INTO master.warehouse BY NAME
SELECT w_warehouse_sk AS warehouse_id,
       w_warehouse_id AS warehouse_code,
       w_warehouse_name AS warehouse_name,
       w_city AS city,
       w_state AS state
FROM tpcds.warehouse;
