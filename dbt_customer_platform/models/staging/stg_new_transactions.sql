SELECT
    CAST(5010 AS BIGINT) AS transaction_id,
    CAST(102 AS BIGINT) AS customer_id,
    CAST(2500 AS DECIMAL(18,2)) AS amount,
    'SUCCESS' AS status

UNION ALL

SELECT
    CAST(5011 AS BIGINT) AS transaction_id,
    CAST(104 AS BIGINT) AS customer_id,
    CAST(1800 AS DECIMAL(18,2)) AS amount,
    'PENDING' AS status