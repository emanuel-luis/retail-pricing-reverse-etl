-- One row per item version x zone, versions in surrogate-key order: valid from the shifted
-- version start, valid to the next version's start; the current version's row is open and carries the seed zone price.
INSERT INTO pricing.price_history BY NAME
WITH v AS (
    SELECT m.product_id, z.zone_id, i.i_item_sk, shift_date(i.i_rec_start_date) AS vs,
           grid_round(i.i_current_price * z.price_factor) AS version_price,
           i.i_item_sk IN (SELECT i_item_sk FROM build.item_current) AS is_current
    FROM tpcds.item i
    JOIN build.item_map m ON m.item_sk = i.i_item_sk
    CROSS JOIN master.price_zone z
),
h AS (
    SELECT v.*,
           lead(vs) OVER (PARTITION BY product_id, zone_id ORDER BY i_item_sk) AS next_vs
    FROM v
)
SELECT row_number() OVER (ORDER BY h.product_id, h.zone_id, h.i_item_sk) AS history_id,
       h.product_id,
       h.zone_id,
       CASE WHEN h.is_current THEN zp.price ELSE h.version_price END AS price,
       CAST(h.vs AS TIMESTAMPTZ) AS valid_from,
       CASE WHEN h.is_current THEN NULL ELSE CAST(h.next_vs AS TIMESTAMPTZ) END AS valid_to,
       'seed' AS source,
       NULL AS cycle_id
FROM h
JOIN pricing.zone_price zp USING (product_id, zone_id);
