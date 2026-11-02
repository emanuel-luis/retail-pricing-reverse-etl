-- Store sales by ticket and web sales by order. A line needs a sold date, an item and a
-- quantity; the rest is dropped and counted. Null measures fall back to the list price, the
-- item price or quantity * unit price. Catalog sales are not used.
CREATE TABLE build.line_src AS
WITH s AS (
    SELECT 'STORE' AS channel, ss_ticket_number AS doc_no, ss_item_sk AS item_sk,
           ss_sold_date_sk AS date_sk, ss_store_sk AS store_sk, ss_customer_sk AS customer_sk,
           ss_promo_sk AS promo_sk, ss_quantity AS quantity, ss_sales_price AS sales_price,
           ss_list_price AS list_price, ss_ext_discount_amt AS ext_discount,
           ss_ext_sales_price AS ext_sales, ss_wholesale_cost AS wholesale_cost
    FROM tpcds.store_sales
    UNION ALL
    SELECT 'WEB', ws_order_number, ws_item_sk, ws_sold_date_sk, NULL, ws_bill_customer_sk,
           ws_promo_sk, ws_quantity, ws_sales_price, ws_list_price, ws_ext_discount_amt,
           ws_ext_sales_price, ws_wholesale_cost
    FROM tpcds.web_sales
)
SELECT s.*, d.d AS business_date, m.product_id, i.i_current_price, i.i_wholesale_cost,
       CASE WHEN s.date_sk IS NULL THEN 'null_sold_date'
            WHEN s.item_sk IS NULL THEN 'null_item'
            WHEN s.quantity IS NULL THEN 'null_quantity'
            WHEN d.d IS NULL THEN 'unknown_date'
            WHEN m.product_id IS NULL THEN 'unknown_item'
       END AS drop_rule
FROM s
LEFT JOIN build.day d ON d.date_sk = s.date_sk
LEFT JOIN build.item_map m ON m.item_sk = s.item_sk
LEFT JOIN tpcds.item i ON i.i_item_sk = s.item_sk;

INSERT INTO build.dropped
SELECT 'sales.sale_line', lower(channel) || '_sales.' || drop_rule, count(*)
FROM build.line_src WHERE drop_rule IS NOT NULL GROUP BY ALL;

-- Headers: one per (channel, document), ids ordered by (channel, document). A ticket with no
-- store goes to store 1 + floor(u(ticket) * stores), so volume is kept.
CREATE TABLE build.sale_hdr AS
WITH n AS (SELECT count(*) AS stores FROM master.store)
SELECT row_number() OVER (ORDER BY h.channel, h.doc_no) AS sale_id, h.*,
       CASE WHEN h.channel = 'WEB' THEN NULL
            ELSE coalesce(sm.store_id, 1 + CAST(floor(u(h.doc_no, 'store') * n.stores) AS INTEGER))
       END AS store_id
FROM (
    SELECT channel, doc_no, max(business_date) AS business_date, max(store_sk) AS store_sk,
           max(customer_sk) AS customer_id
    FROM build.line_src WHERE drop_rule IS NULL GROUP BY ALL
) h
CROSS JOIN n
LEFT JOIN build.store_map sm ON sm.store_sk = h.store_sk;

CREATE TABLE build.line AS
SELECT h.sale_id,
       l.channel, l.doc_no, l.item_sk,
       row_number() OVER (PARTITION BY l.channel, l.doc_no ORDER BY l.item_sk) AS line_no,
       l.product_id,
       l.quantity,
       coalesce(l.sales_price, l.list_price, l.i_current_price) AS unit_price,
       coalesce(l.ext_discount,
                greatest(l.list_price - l.sales_price, 0) * l.quantity, 0) AS discount_amount,
       coalesce(l.ext_sales,
                coalesce(l.sales_price, l.list_price, l.i_current_price) * l.quantity)
           AS net_amount,
       coalesce(l.wholesale_cost, l.i_wholesale_cost) AS unit_cost,
       l.promo_sk AS promo_id
FROM build.line_src l
JOIN build.sale_hdr h USING (channel, doc_no)
WHERE l.drop_rule IS NULL;

-- sold_at = business date + 08:00 + floor(u * 14 h), UTC.
INSERT INTO sales.sale BY NAME
SELECT h.sale_id, h.channel, h.store_id, h.customer_id, h.business_date,
       CAST(h.business_date AS TIMESTAMP) + INTERVAL 8 HOUR
           + to_seconds(CAST(floor(u(h.channel || ':' || h.doc_no, 'sold_at') * 50400) AS BIGINT))
           AS sold_at,
       t.total_amount
FROM build.sale_hdr h
JOIN (SELECT sale_id, sum(net_amount) AS total_amount FROM build.line GROUP BY 1) t
  USING (sale_id);
