-- Dense class id per (category, TPC-DS class).
CREATE TABLE build.class_map AS
SELECT dense_rank() OVER (ORDER BY category_id, class_key) AS class_id, *
FROM (
    SELECT coalesce(i_category_id, 99) AS category_id,
           coalesce(i_class_id, -1) AS class_key,
           coalesce(min(i_class), 'Other') AS class_name
    FROM build.item_current
    GROUP BY ALL
);

INSERT INTO master.product_class BY NAME
SELECT class_id, category_id, class_name FROM build.class_map;
