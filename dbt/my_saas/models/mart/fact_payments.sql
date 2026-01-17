{{
    config(
        materialized='table',
        schema='gold',
    )
}}

select
    f.payment_id as payment_id,
    c.customer_key,
    pr.product_key,
    co.country_key,
    d.date_key as order_date_key,
    f.amount - coalesce(f.fee,0) - coalesce(f.refunded_amount,0) as net_revenue,
    f.amount,
    f.fee,
    f.refunded_amount,
    f.payment_method,
    f.status,
    f.updated_at
from {{ ref('stg_payments') }} f
left join {{ ref('dim_customer') }} c
    on f.customer_id = c.customer_id
left join {{ ref('dim_product') }} pr
    on f.product = pr.product_name
left join {{ ref('dim_country') }} co
    on f.country = co.country_name
left join {{ ref('dim_date') }} d
    on f.created_at::date = d.full_date
