{{config(
    materialized='view'
)}}

WITH ranked AS (
    SELECT
        v:customer_id::STRING AS customer_id,
        v:customer_first_name::STRING AS customer_first_name,
        v:customer_last_name::STRING AS customer_last_name,
        v:customer_email::STRING AS customer_email,
        v:created_at::TIMESTAMP AS created_at,
        current_timestamp() AS load_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY v:customer_id::STRING 
            ORDER BY v:created_at DESC
            ) AS rn

    FROM {{source('raw_data', 'customers')}}
)
SELECT 
    customer_id,
    customer_first_name,
    customer_last_name,
    customer_email,
    created_at,
    load_timestamp
FROM ranked
WHERE rn = 1