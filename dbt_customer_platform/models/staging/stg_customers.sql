
SELECT
    customer_id,
    customer_name,
    city,
    email,
    email_missing_flag
FROM {{ source('silver', 'customers') }}
