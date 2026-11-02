-- Current item version per i_item_id; pack_units from the size (build.pack_units, from synth.py).
INSERT INTO master.product BY NAME
SELECT m.product_id,
       i.i_item_id AS product_code,
       coalesce(i.i_product_name, i.i_item_id) AS product_name,
       c.class_id,
       b.brand_id,
       s.supplier_id,
       i.i_size AS size,
       coalesce(pu.units, 1) AS pack_units,
       true AS is_active
FROM build.item_current i
JOIN build.item_map m ON m.item_sk = i.i_item_sk
JOIN build.class_map c
  ON c.category_id = coalesce(i.i_category_id, 99) AND c.class_key = coalesce(i.i_class_id, -1)
LEFT JOIN build.brand_map b USING (i_brand_id)
LEFT JOIN build.supplier_map s USING (i_manufact_id)
LEFT JOIN build.pack_units pu ON pu.size = i.i_size;
