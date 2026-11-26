
SELECT * FROM customer_data_platform.bronze.customers;

SELECT * FROM customer_data_platform.bronze.transactions;


SELECT 'CUSTOMERS' AS table_name, COUNT(*) AS total_records
FROM customer_data_platform.bronze.customers

UNION ALL

SELECT 'TRANSACTIONS', COUNT(*)
FROM customer_data_platform.bronze.transactions;

SELECT
    customer_id,
    customer_name,
    ingestion_timestamp,
    source_file_name,
    batch_id
FROM customer_data_platform.bronze.customers;

SELECT *
FROM customer_data_platform.bronze.transactions
WHERE transaction_id = 5008;