SELECT *
FROM customer_data_platform.gold.stg_customers;

SELECT *
FROM customer_data_platform.gold.stg_transactions;

SELECT *
FROM customer_data_platform.gold.int_customer_profile;

SELECT *
FROM customer_data_platform.gold.dim_customer_dbt
ORDER BY customer_id;

SELECT *
FROM customer_data_platform.gold.fact_transactions_dbt
ORDER BY transaction_id;

SELECT
    COUNT(*) AS total_transactions,
    SUM(amount) AS total_amount
FROM customer_data_platform.gold.fact_transactions_dbt;

SELECT *
FROM customer_data_platform.gold.customer_360_dbt
ORDER BY customer_id;

SELECT
    COUNT(*) AS total_customers,
    SUM(total_transactions) AS total_transactions,
    SUM(total_transaction_amount) AS total_amount,
    SUM(successful_transactions) AS successful_transactions,
    SUM(failed_transactions) AS failed_transactions,
    SUM(pending_transactions) AS pending_transactions
FROM customer_data_platform.gold.customer_360_dbt;

------------------------------------------------------------------------------
--Incremental query
------------------------------------------------------------------------------
SELECT
    COUNT(*) AS total_records,
    SUM(amount) AS total_amount
FROM customer_data_platform.gold.fact_transactions_incremental;

SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT transaction_id) AS unique_transactions,
    SUM(amount) AS total_amount
FROM customer_data_platform.gold.fact_transactions_incremental;

SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT transaction_id) AS unique_transactions,
    SUM(amount) AS total_amount
FROM customer_data_platform.gold.fact_transactions_merge_dbt;

SELECT *
FROM customer_data_platform.gold.fact_transactions_merge_dbt
WHERE transaction_id = 5010;

SELECT transaction_id, amount
FROM customer_data_platform.gold.fact_transactions_merge_dbt
WHERE transaction_id = 5010;

SELECT
    transaction_id,
    customer_id,
    amount,
    status
FROM customer_data_platform.gold.fact_transactions_merge_dbt
WHERE transaction_id = 5010;

SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT transaction_id) AS unique_transactions,
    SUM(amount) AS total_amount
FROM customer_data_platform.gold.fact_transactions_merge_dbt;

------------------------------------------------------------------------------
--SCD2
------------------------------------------------------------------------------

SELECT
    customer_id,
    customer_name,
    city,
    dbt_valid_from,
    dbt_valid_to
FROM customer_data_platform.gold.snap_customer_profile
ORDER BY customer_id;

SELECT
    customer_id,
    customer_name,
    city,
    dbt_valid_from,
    dbt_valid_to
FROM customer_data_platform.gold.snap_customer_profile
WHERE customer_id = 102
ORDER BY dbt_valid_from;

SELECT
    customer_id,
    customer_name,
    city,
    dbt_valid_from,
    dbt_valid_to,
    CASE
        WHEN dbt_valid_to IS NULL THEN 'Y'
        ELSE 'N'
    END AS current_flag
FROM customer_data_platform.gold.snap_customer_profile
WHERE customer_id = 102
ORDER BY dbt_valid_from;