
SELECT 'SILVER_CUSTOMERS' AS table_name,
       COUNT(*) AS total_records
FROM customer_data_platform.silver.customers

UNION ALL

SELECT 'CUSTOMER_EXCEPTIONS',
       COUNT(*)
FROM customer_data_platform.silver.customer_exceptions;
------------------------------------------------------------------------------

SELECT 'VALID_TRANSACTIONS' AS table_name, COUNT(*) AS total_records
FROM customer_data_platform.silver.transactions

UNION ALL

SELECT 'DUPLICATE_TRANSACTIONS', COUNT(*)
FROM customer_data_platform.silver.transaction_exceptions

UNION ALL

SELECT 'NEGATIVE_TRANSACTIONS', COUNT(*)
FROM customer_data_platform.silver.negative_transaction_exceptions

UNION ALL

SELECT 'AMBIGUOUS_CUSTOMERS', COUNT(*)
FROM customer_data_platform.silver.ambiguous_customer_exceptions

UNION ALL

SELECT 'UNKNOWN_CUSTOMERS', COUNT(*)
FROM customer_data_platform.silver.unknown_customer_exceptions;


SHOW TABLES IN customer_data_platform.silver;

