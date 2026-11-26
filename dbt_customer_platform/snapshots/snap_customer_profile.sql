{% snapshot snap_customer_profile %}

{{
    config(
        target_schema='gold',
        unique_key='customer_id',
        strategy='check',
        check_cols=[
            'customer_name',
            'city',
            'email',
            'email_missing_flag'
        ]
    )
}}

SELECT
    customer_id,
    customer_name,
    city,
    email,
    email_missing_flag
FROM {{ ref('stg_customer_snapshot_source') }}

{% endsnapshot %}