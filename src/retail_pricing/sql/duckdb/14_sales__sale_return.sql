-- Returns link to the sold line by (ticket or order, item). A return needs a date, a quantity
-- and a kept line; returns dated after the anchor are dropped too. All drops are counted.
CREATE TABLE build.return_src AS
WITH r AS (
    SELECT 'STORE' AS channel, sr_ticket_number AS doc_no, sr_item_sk AS item_sk,
           sr_returned_date_sk AS date_sk, sr_return_quantity AS quantity,
           sr_return_amt AS amount
    FROM tpcds.store_returns
    UNION ALL
    SELECT 'WEB', wr_order_number, wr_item_sk, wr_returned_date_sk, wr_return_quantity,
           wr_return_amt
    FROM tpcds.web_returns
)
SELECT r.*, d.d AS business_date, l.sale_line_id, l.unit_price,
       CASE WHEN r.date_sk IS NULL THEN 'null_returned_date'
            WHEN r.quantity IS NULL THEN 'null_quantity'
            WHEN d.d IS NULL THEN 'unknown_date'
            WHEN l.sale_line_id IS NULL THEN 'no_sale_line'
            WHEN d.d > anchor() THEN 'after_anchor'
       END AS drop_rule
FROM r
LEFT JOIN build.day d ON d.date_sk = r.date_sk
LEFT JOIN (
    SELECT b.channel, b.doc_no, b.item_sk, b.unit_price, s.sale_line_id
    FROM build.line b JOIN sales.sale_line s USING (sale_id, line_no)
) l USING (channel, doc_no, item_sk);

INSERT INTO build.dropped
SELECT 'sales.sale_return', lower(channel) || '_returns.' || drop_rule, count(*)
FROM build.return_src WHERE drop_rule IS NOT NULL GROUP BY ALL;

INSERT INTO sales.sale_return BY NAME
SELECT row_number() OVER (
           ORDER BY channel, doc_no, item_sk, date_sk, quantity, amount) AS return_id,
       sale_line_id,
       quantity,
       coalesce(amount, quantity * unit_price) AS amount,
       business_date
FROM build.return_src WHERE drop_rule IS NULL;
