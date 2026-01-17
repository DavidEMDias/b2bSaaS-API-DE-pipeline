{{
    config(
        materialized='table',
        schema='gold',
    )
}}

select
    d.date_key as session_date_key,
    dev.device_key,
    co.country_key,
    count(distinct s.session_id) as total_sessions,
    sum(s.pageviews) as total_pageviews,
    avg(s.session_duration_s) as avg_session_duration_s,
    avg(cast(s.bounced as int)) as bounce_rate,
    avg(cast(s.converted as int)) as conversion_rate
from {{ ref('fact_sessions') }} s
left join {{ ref('dim_date') }} d on s.session_date_key = d.date_key
left join {{ ref('dim_device') }} dev on s.device_key = dev.device_key
left join {{ ref('dim_country') }} co on s.country_key = co.country_key
group by d.date_key, dev.device_key, co.country_key
