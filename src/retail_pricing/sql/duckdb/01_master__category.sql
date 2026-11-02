-- TPC-DS categories (current item versions) plus 99 "Other" for null categories.
INSERT INTO master.category BY NAME
SELECT i_category_id AS category_id,
       coalesce(min(i_category), 'Category ' || i_category_id) AS category_name
FROM build.item_current
WHERE i_category_id IS NOT NULL AND i_category_id <> 99
GROUP BY i_category_id
UNION ALL
SELECT 99, 'Other';
