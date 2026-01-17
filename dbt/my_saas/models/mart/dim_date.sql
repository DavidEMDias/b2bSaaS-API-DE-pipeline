{{
    config(
        materialized='table',
        schema='gold',
    )
}}

with dates as (
    select distinct order_day as date
    from {{ ref('stg_payments') }}
    union
    select distinct date_trunc('day', session_start)::date as date
    from {{ ref('stg_sessions') }}
)

select
    dense_rank() over (order by date) as date_key,
    date as full_date,
    extract(year from date) as year,
    extract(month from date) as month,
    extract(day from date) as day,
    extract(dow from date) as day_of_week
from dates
