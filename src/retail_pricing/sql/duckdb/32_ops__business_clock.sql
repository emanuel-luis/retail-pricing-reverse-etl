INSERT INTO ops.business_clock BY NAME
SELECT 1 AS id, anchor() AS business_date;
