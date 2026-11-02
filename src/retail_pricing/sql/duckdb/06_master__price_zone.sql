-- Z = min(8, max(1, round(stores / 4))) store zones plus one WEB zone (id Z + 1).
-- price_factor = 0.96 + 0.02 * (zone_no mod 5).
INSERT INTO master.price_zone BY NAME
WITH z AS (SELECT least(8, greatest(1, (count(*) + 2) // 4)) AS n FROM build.store_current)
SELECT zone_id,
       CASE WHEN zone_id <= n THEN 'Z' || lpad(zone_id::VARCHAR, 2, '0') ELSE 'WEB' END AS zone_code,
       CASE WHEN zone_id <= n THEN 'STORE' ELSE 'WEB' END AS channel,
       0.96 + 0.02 * (zone_id % 5) AS price_factor
FROM z, range(1, n + 2) r(zone_id);
