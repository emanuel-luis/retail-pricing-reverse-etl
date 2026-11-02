-- Re-timed around the anchor: start = anchor - 14 + floor(42u), 7..28 days, depth 10..30%.
INSERT INTO pricing.promotion BY NAME
SELECT p_promo_sk AS promo_id,
       p_promo_id AS promo_code,
       anchor() - 14 + CAST(floor(42 * u(p_promo_sk, 'promo_start')) AS INTEGER) AS start_date,
       anchor() - 14 + CAST(floor(42 * u(p_promo_sk, 'promo_start')) AS INTEGER)
           + 6 + CAST(floor(22 * u(p_promo_sk, 'promo_length')) AS INTEGER) AS end_date,
       CAST(10 + floor(21 * u(p_promo_sk, 'promo_depth')) AS INTEGER) * 0.01 AS discount_pct,
       'ALL' AS channel
FROM tpcds.promotion;
