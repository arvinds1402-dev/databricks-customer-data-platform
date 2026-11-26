{{
    config(
        materialized='incremental',
        unique_key='transaction_id'
    )
}}

WITH all_transactions AS (

    SELECT
        transaction_id,
        customer_id,
        amount,
        status
    FROM {{ ref('stg_transactions') }}

    UNION ALL

    SELECT
        transaction_id,
        customer_id,
        amount,
        status
    FROM {{ ref('stg_new_transactions') }}

)

SELECT *
FROM all_transactions

{% if is_incremental() %}

WHERE transaction_id NOT IN (
    SELECT transaction_id
    FROM {{ this }}
)

{% endif %}