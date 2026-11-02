-- master.customer, latest version per primary key (customer_id).
select customer_id,
       customer_code,
       first_name,
       last_name,
       email,
       city,
       state,
       zip,
       version,
       updated_at
from {{ source('raw', 'master__customer') }} final
