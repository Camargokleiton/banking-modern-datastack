{{ config(
    materialized='table'
) }}

WITH latest AS (
    SELECT
        customer_id,
        customer_first_name,
        customer_last_name,
        customer_email,
        created_at,
        dbt_valid_from as effective_from,
        dbt_valid_to as effective_to,
        case when dbt_valid_to is null then true else false end as is_current
    FROM {{ ref('customer_snapshot') }}
)
SELECT * FROM latest