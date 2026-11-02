-- One brand per TPC-DS brand id; tier: 1 private label (u < 0.15), 3 premium (u > 0.85), else 2.
CREATE TABLE build.brand_map AS
SELECT i_brand_id, dense_rank() OVER (ORDER BY i_brand_id) AS brand_id, min(i_brand) AS brand_name
FROM build.item_current
WHERE i_brand_id IS NOT NULL
GROUP BY i_brand_id;

INSERT INTO master.brand BY NAME
SELECT brand_id,
       coalesce(brand_name, 'Brand ' || i_brand_id) AS brand_name,
       CASE WHEN u(i_brand_id, 'brand_tier') < 0.15 THEN 1
            WHEN u(i_brand_id, 'brand_tier') > 0.85 THEN 3
            ELSE 2 END AS brand_tier
FROM build.brand_map;
