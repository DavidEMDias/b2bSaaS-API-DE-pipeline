{{
    config(
        materialized='table',
        schema='gold',
    )
}}

select
    c.customer_key,
    c.country,
    c.industry,
    c.company_size,
    count(distinct f.payment_id) as total_orders,
    sum(f.net_revenue) as total_revenue,
    max(s.session_start) as last_session,
    count(distinct s.session_id) as total_sessions,
    c.signup_date,
    c.is_churned
from {{ ref('dim_customer') }} c
left join {{ ref('fact_payments') }} f on c.customer_id = f.customer_id
left join {{ ref('fact_sessions') }} s on c.customer_id = s.customer_id
group by c.customer_key, c.country, c.industry, c.company_size, c.signup_date, c.is_churned
