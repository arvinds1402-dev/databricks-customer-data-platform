
SELECT
    transaction_id,
    customer_id,
    amount,
    status
FROM {{ source('silver', 'transactions') }}
