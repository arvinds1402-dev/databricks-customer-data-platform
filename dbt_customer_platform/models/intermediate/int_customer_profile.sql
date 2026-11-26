SELECT
    customer_id,
    customer_name,
    city,
    email,
    email_missing_flag
FROM {{ ref('stg_customers') }}