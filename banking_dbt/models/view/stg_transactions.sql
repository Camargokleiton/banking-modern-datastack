{{config(
    materialized='view'
)}}


    SELECT
        v:transactions_id::STRING AS transactions_id,
        v:account_id::STRING AS account_id,
        v:txn_type::STRING AS txn_type,
        v:amount::float AS amount,
        v:related_account_id::STRING AS related_account_id,
        v:status::STRING AS status,
        v:created_at::TIMESTAMP AS created_at,
        current_timestamp() AS load_timestamp
    FROM {{source('raw_data', 'transactions')}}
