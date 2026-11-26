SELECT
    t.transaction_id,
    t.customer_id,
    c.customer_name,
    c.city,
    t.amount,
    t.status
FROM {{ ref('stg_transactions') }} AS t
INNER JOIN {{ ref('dim_customer_dbt') }} AS c
    ON t.customer_id = c.customer_id