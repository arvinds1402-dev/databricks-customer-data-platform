SELECT
    c.customer_id,
    c.customer_name,
    c.city,
    c.email,
    COUNT(f.transaction_id) AS total_transactions,
    COALESCE(SUM(f.amount), 0) AS total_transaction_amount,
    SUM(
        CASE
            WHEN f.status = 'SUCCESS' THEN 1
            ELSE 0
        END
    ) AS successful_transactions,
    SUM(
        CASE
            WHEN f.status = 'FAILED' THEN 1
            ELSE 0
        END
    ) AS failed_transactions,
    SUM(
        CASE
            WHEN f.status = 'PENDING' THEN 1
            ELSE 0
        END
    ) AS pending_transactions
FROM {{ ref('dim_customer_dbt') }} AS c
LEFT JOIN {{ ref('fact_transactions_dbt') }} AS f
    ON c.customer_id = f.customer_id
GROUP BY
    c.customer_id,
    c.customer_name,
    c.city,
    c.email