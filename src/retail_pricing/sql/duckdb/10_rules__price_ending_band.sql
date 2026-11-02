-- Static: [0,10) ends .49/.99, [10,50) ends .99, [50,inf) ends .90.
INSERT INTO rules.price_ending_band BY NAME
SELECT * FROM (VALUES
    (1, 0.00, 10.00, 0.49),
    (2, 0.00, 10.00, 0.99),
    (3, 10.00, 50.00, 0.99),
    (4, 50.00, NULL, 0.90)
) v(band_id, min_price, max_price, ending);
