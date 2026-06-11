{{config(
    materialized='view'
)}}

WITH ranked AS (
    SELECT
        v:account_id::STRING AS account_id,
        v:customer_id::STRING AS customer_id,
        v:account_type::STRING AS account_type,
        v:balance::STRING AS balance,
        v:currency::STRING AS currency,
        v:created_at::TIMESTAMP AS created_at,
        current_timestamp() AS load_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY v:account_id::STRING 
            ORDER BY v:created_at DESC
            ) AS rn

    FROM {{source('raw_data', 'accounts')}}
)
SELECT 
    account_id,
    customer_id,
    account_type,
    balance,
    currency,
    created_at,
    load_timestamp
FROM ranked
WHERE rn = 1