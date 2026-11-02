-- One supplier per TPC-DS manufacturer.
CREATE TABLE build.supplier_map AS
SELECT i_manufact_id, dense_rank() OVER (ORDER BY i_manufact_id) AS supplier_id,
       min(i_manufact) AS supplier_name
FROM build.item_current
WHERE i_manufact_id IS NOT NULL
GROUP BY i_manufact_id;

INSERT INTO master.supplier BY NAME
SELECT supplier_id, coalesce(supplier_name, 'Supplier ' || i_manufact_id) AS supplier_name
FROM build.supplier_map;
