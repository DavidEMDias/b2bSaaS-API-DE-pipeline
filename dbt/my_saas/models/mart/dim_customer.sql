{{
    config(
        materialized='table',
        schema='gold',
    )
}}

with customers as (
    select distinct
        customer_id,
        company_name,
        country,
        industry,
        company_size,
        signup_date,
        updated_at,
        is_churned
    from {{ ref('stg_customers') }}
    where customer_id is not null
)

select
    dense_rank() over (order by customer_id) as customer_key,  -- surrogate key
    customer_id,
    company_name,
    country,
    industry,
    company_size,
    signup_date,
    updated_at,
    is_churned
from customers
