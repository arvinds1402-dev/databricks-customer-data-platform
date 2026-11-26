
SELECT *
FROM customer_data_platform.gold.dim_customer;



SELECT
    'DIM_CUSTOMER' AS table_name,
    COUNT(*) AS total_records
FROM customer_data_platform.gold.dim_customer

UNION ALL

SELECT
    'FACT_TRANSACTIONS',
    COUNT(*)
FROM customer_data_platform.gold.fact_transactions;


SELECT current_user();



-- Gold layer record count validation

SELECT
    'DIM_CUSTOMER' AS table_name,
    COUNT(*) AS total_records
FROM customer_data_platform.gold.dim_customer

UNION ALL

SELECT
    'FACT_TRANSACTIONS',
    COUNT(*)
FROM customer_data_platform.gold.fact_transactions

UNION ALL

SELECT
    'CUSTOMER_360',
    COUNT(*)
FROM customer_data_platform.gold.customer_360;

SELECT
    customer_id,
    customer_name,
    total_transactions,
    total_transaction_amount,
    successful_transactions,
    successful_transaction_amount,
    failed_transactions
FROM customer_data_platform.gold.customer_360
ORDER BY customer_id;

