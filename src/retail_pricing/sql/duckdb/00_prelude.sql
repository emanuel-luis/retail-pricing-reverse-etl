-- Shared helpers in schema `build` (never exported).

-- TPC-DS date keys to shifted business dates.
CREATE TABLE build.day AS
SELECT d_date_sk AS date_sk, shift_date(d_date) AS d FROM tpcds.date_dim;

-- One current version per business key (the latest start date wins).
CREATE TABLE build.item_current AS
SELECT * FROM tpcds.item
QUALIFY row_number() OVER (
    PARTITION BY i_item_id ORDER BY i_rec_start_date DESC NULLS LAST, i_item_sk DESC) = 1;

CREATE TABLE build.store_current AS
SELECT * FROM tpcds.store
QUALIFY row_number() OVER (
    PARTITION BY s_store_id ORDER BY s_rec_start_date DESC NULLS LAST, s_store_sk DESC) = 1;

-- Every item version maps to its product (dense rank of i_item_id).
CREATE TABLE build.item_map AS
WITH p AS (
    SELECT i_item_id, dense_rank() OVER (ORDER BY i_item_id) AS product_id
    FROM build.item_current
)
SELECT i.i_item_sk AS item_sk, p.product_id
FROM tpcds.item i JOIN p USING (i_item_id);
