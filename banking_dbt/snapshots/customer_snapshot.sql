{%snapshot customer_snapshot%}
    {{
        config(
            target_schema='ANALYTICS',
            unique_key='customer_id',
            strategy='check',
            check_cols=['customer_first_name', 'customer_last_name', 'customer_email']
        )
    }}
    
    select *
    from {{ ref('stg_customers') }}
    
{%endsnapshot%}