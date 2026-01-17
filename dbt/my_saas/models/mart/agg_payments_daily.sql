{{
    config(
        materialized='table',
        schema='gold',
    )
}}

select
    order_date_key,
    product_key,
    country_key,
    sum(net_revenue) as total_net_revenue,
    sum(amount) as total_amount,
    count(payment_id) as total_orders,
    count(distinct customer_key) as active_customers
from {{ ref('fact_payments') }}
group by order_date_key, product_key, country_key
