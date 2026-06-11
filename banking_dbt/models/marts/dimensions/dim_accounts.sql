{{ config(
    materialized='table'
) }}

WITH latest AS (
    SELECT
        account_id,
        customer_id,
        account_type,
        balance,
        currency,
        created_at,
        dbt_valid_from as effective_from,
        dbt_valid_to as effective_to,
        case when dbt_valid_to is null then true else false end as is_current
    FROM {{ ref('account_snapshot') }}
)
SELECT * FROM latest