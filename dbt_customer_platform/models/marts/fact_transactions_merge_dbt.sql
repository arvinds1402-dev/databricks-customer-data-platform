{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='transaction_id'
    )
}}

SELECT
    transaction_id,
    customer_id,
    amount,
    status
FROM {{ ref('stg_transactions_merge_source') }}