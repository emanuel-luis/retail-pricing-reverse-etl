INSERT INTO master.customer BY NAME
SELECT c.c_customer_sk AS customer_id,
       c.c_customer_id AS customer_code,
       c.c_first_name AS first_name,
       c.c_last_name AS last_name,
       c.c_email_address AS email,
       a.ca_city AS city,
       a.ca_state AS state,
       a.ca_zip AS zip
FROM tpcds.customer c
LEFT JOIN tpcds.customer_address a ON a.ca_address_sk = c.c_current_addr_sk;
