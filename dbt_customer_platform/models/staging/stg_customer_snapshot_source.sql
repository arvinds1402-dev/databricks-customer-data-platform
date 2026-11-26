SELECT
    customer_id,
    customer_name,

    CASE
        WHEN customer_id = 102 THEN 'Chennai'
        ELSE city
    END AS city,

    email,
    email_missing_flag

FROM {{ ref('stg_customers') }}