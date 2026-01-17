{{
    config(
        materialized='table',
        schema='gold',
    )
}}

with countries as (
    select distinct country
    from (
        select country from {{ ref('stg_customers') }}
        union
        select country from {{ ref('stg_payments') }}
        union
        select country from {{ ref('stg_sessions') }}
    ) all_countries
    where country is not null
)

select
    dense_rank() over (order by country) as country_key,
    country as country_name
from countries