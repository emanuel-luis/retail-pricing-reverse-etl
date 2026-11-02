INSERT INTO market.competitor BY NAME
SELECT * FROM (VALUES
    (1, 'Discounter', -0.04),
    (2, 'Regional Grocer', 0.01),
    (3, 'Premium Market', 0.05)
) v(competitor_id, competitor_name, price_bias);
