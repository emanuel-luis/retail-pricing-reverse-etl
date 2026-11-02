INSERT INTO sales.sale_line BY NAME
SELECT row_number() OVER (ORDER BY sale_id, line_no) AS sale_line_id,
       sale_id, line_no, product_id, quantity, unit_price, discount_amount, net_amount,
       unit_cost, promo_id
FROM build.line;
