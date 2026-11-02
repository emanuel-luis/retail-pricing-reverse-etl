-- Current store versions, dense id by s_store_id; zone = ((store_id - 1) mod Z) + 1.
-- price_adjust_pct = 0 for 70% of stores, else 1..4%.
INSERT INTO master.store BY NAME
WITH z AS (SELECT count(*) AS n FROM master.price_zone WHERE channel = 'STORE'),
s AS (SELECT dense_rank() OVER (ORDER BY s_store_id) AS store_id, * FROM build.store_current)
SELECT store_id,
       s_store_id AS store_code,
       s_store_name AS store_name,
       (store_id - 1) % n + 1 AS zone_id,
       s_city AS city,
       s_state AS state,
       CASE WHEN u(store_id, 'store_adjust') < 0.7 THEN 0
            ELSE CAST(1 + floor(4 * u(store_id, 'store_adjust_size')) AS INTEGER) * 0.01
       END AS price_adjust_pct
FROM s, z;

-- Every store version maps to its store.
CREATE TABLE build.store_map AS
SELECT t.s_store_sk AS store_sk, s.store_id
FROM tpcds.store t JOIN master.store s ON s.store_code = t.s_store_id;
