{{
    config(
        materialized='table',
        schema='gold',
    )
}}

with products as (
    select distinct
        product,
        min(updated_at) as created_at,
        max(updated_at) as updated_at
    from {{ ref('stg_payments') }}
    where product is not null
    group by product
)

select
    dense_rank() over (order by product) as product_key,
    product as product_name,
    created_at,
    updated_at
from products
