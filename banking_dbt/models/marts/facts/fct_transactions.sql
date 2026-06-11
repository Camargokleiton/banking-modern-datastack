{{ config(
    materialized='incremental'
) }}

SELECT
    t.transactions_id,
    t.account_id,
    t.amount,
    a.customer_id,
    t.related_account_id,
    t.status,
    t.txn_type,
    t.created_at as transaction_time,
    current_timestamp() as load_timestamp
FROM {{ ref('stg_transactions') }} t
LEFT JOIN {{ ref('dim_accounts') }} a
ON t.account_id = a.account_id


